// biome-ignore-all lint/correctness/useImportExtensions: Bun and TypeScript resolve package source imports extensionlessly.
// biome-ignore-all lint/style/useNamingConvention: MCP protocol fields retain their wire names.
// biome-ignore-all lint/performance/noAwaitInLoops: Stream reads must be drained sequentially.
// biome-ignore-all lint/correctness/useQwikValidLexicalScope: This package shares checks with Qwik hosts but creates no Qwik components.
// biome-ignore-all lint/complexity/noExcessiveLinesPerFunction: Startup owns a single bounded child-generation sequence.
// biome-ignore-all lint/complexity/noExcessiveCognitiveComplexity: Startup owns one ordered readiness and cleanup lifecycle.
// biome-ignore-all lint/style/noExcessiveLinesPerFile: Startup readiness and owned child streams share one lifecycle boundary.
// biome-ignore-all lint/style/noMagicNumbers: Wire delimiters and exact DTO field counts are protocol constants.
// biome-ignore-all lint/complexity/useLiteralKeys: DTO validation operates on untrusted indexed objects.
// biome-ignore-all lint/style/noParameterProperties: FrameReader captures immutable stream bounds.
// biome-ignore-all lint/style/useBlockStatements: Early exits keep protocol checks linear.
// biome-ignore-all lint/style/noContinue: Frame scanning skips non-delimiter bytes.
// biome-ignore-all lint/style/useExplicitLengthCheck: Empty buffers are explicit protocol states.
// biome-ignore-all lint/suspicious/noUnnecessaryConditions: Stream EOF is a runtime state transition.
// biome-ignore-all lint/style/noIncrementDecrement: Sequence numbers are monotonic protocol identifiers.
// biome-ignore-all lint/style/noTernary: Context mode and fallback values are compact protocol guards.
// biome-ignore-all lint/complexity/useSimplifiedLogicExpression: Exact result-shape checks are intentionally explicit.
import type {
  EnabledContextSettings,
  ResolvedContextConfig,
} from "./context-config";

const PROTOCOL_VERSION = "2026-07-28";
const SERVER_NAME = "total-recall-mcp";
const PROTOCOL_META = "io.modelcontextprotocol/protocolVersion";
const SERVER_INFO_META = "io.modelcontextprotocol/serverInfo";
const CLIENT_CAPABILITIES_META = "io.modelcontextprotocol/clientCapabilities";
const FAILURE = {
  kind: "failure",
  category: "context_startup_failed",
} as const;

interface RpcResponse {
  readonly jsonrpc?: unknown;
  readonly id?: unknown;
  readonly result?: unknown;
  readonly error?: unknown;
}

interface ContextMemory {
  readonly type: string;
  readonly title: string;
  readonly content: string;
  readonly concepts: readonly string[];
}

function validMemory(value: unknown): value is ContextMemory {
  if (typeof value !== "object" || value === null || Array.isArray(value))
    return false;
  const item = value as Record<string, unknown>;
  return (
    Object.keys(item).length === 4 &&
    typeof item["type"] === "string" &&
    typeof item["title"] === "string" &&
    typeof item["content"] === "string" &&
    Array.isArray(item["concepts"]) &&
    item["concepts"].every((concept) => typeof concept === "string")
  );
}

interface WorkerProcess {
  readonly stdin: {
    write: (data: string) => number | Promise<number>;
    end: () => void;
  };
  readonly stdout: ReadableStream<Uint8Array>;
  readonly stderr: ReadableStream<Uint8Array>;
  readonly exited: Promise<number>;
  kill: (signal?: number | NodeJS.Signals) => void;
}

type WorkerSpawner = (options: {
  readonly cmd: string[];
  readonly cwd?: string;
  readonly env: Record<string, string>;
  readonly stdin: "pipe";
  readonly stdout: "pipe";
  readonly stderr: "pipe";
}) => WorkerProcess;

const expectedInputSchema = {
  type: "object",
  properties: { limit: { type: "integer", minimum: 1, maximum: 50 } },
  required: [],
  additionalProperties: false,
};
const expectedOutputSchema = {
  type: "object",
  properties: {
    memories: {
      type: "array",
      items: {
        type: "object",
        properties: {
          type: { type: "string" },
          title: { type: "string" },
          content: { type: "string" },
          concepts: { type: "array", items: { type: "string" } },
        },
        required: ["type", "title", "content", "concepts"],
        additionalProperties: false,
      },
    },
  },
  required: ["memories"],
  additionalProperties: false,
};

function equalJson(left: unknown, right: unknown): boolean {
  const canonical = (value: unknown): unknown => {
    if (Array.isArray(value)) {
      return value.map(canonical);
    }
    if (typeof value !== "object" || value === null) {
      return value;
    }
    return Object.fromEntries(
      Object.entries(value)
        .sort(([leftKey], [rightKey]) => leftKey.localeCompare(rightKey))
        .map(([key, nested]) => [key, canonical(nested)]),
    );
  };
  return JSON.stringify(canonical(left)) === JSON.stringify(canonical(right));
}

async function drain(stream: ReadableStream<Uint8Array>): Promise<void> {
  const reader = stream.getReader();
  try {
    while (!(await reader.read()).done) {
      // Child stderr is discarded while continuously drained.
    }
  } finally {
    reader.releaseLock();
  }
}

interface StartupReader {
  read: () => Promise<{
    readonly done?: boolean | undefined;
    readonly value?: Uint8Array | undefined;
  }>;
  releaseLock: () => void;
}

class FrameReader {
  private readonly decoder = new TextDecoder("utf-8", { fatal: true });
  private buffered: number[] = [];
  private readonly frames: Uint8Array[] = [];
  private queuedBytes = 0;
  private ended = false;
  private reading: Promise<void> | undefined;

  constructor(
    private readonly reader: StartupReader,
    private readonly cap: number,
  ) {}

  async next(): Promise<RpcResponse> {
    while (this.frames.length === 0) {
      if (this.ended) throw new Error("partial or closed worker frame");
      if (this.reading === undefined) {
        this.reading = this.fill().finally(() => {
          this.reading = undefined;
        });
      }
      await this.reading;
    }
    const frame = this.frames.shift() as Uint8Array;
    this.queuedBytes -= frame.length + 1;
    return JSON.parse(this.decoder.decode(frame)) as RpcResponse;
  }

  release(): void {
    this.reader.releaseLock();
  }

  async discardToEof(): Promise<void> {
    this.frames.length = 0;
    this.buffered = [];
    this.queuedBytes = 0;
    this.ended = false;
    while (true) {
      const chunk = await this.reader.read();
      if (chunk.done) {
        this.ended = true;
        return;
      }
    }
  }

  private async fill(): Promise<void> {
    const chunk = await this.reader.read();
    if (chunk.done) {
      this.ended = true;
      if (this.buffered.length !== 0) throw new Error("partial worker frame");
      return;
    }
    const bytes = chunk.value ?? new Uint8Array();
    for (const byte of bytes) {
      if (this.queuedBytes + this.buffered.length + 1 > this.cap)
        throw new Error("oversized retained worker frames");
      if (byte === 10) {
        const { length } = this.buffered;
        if (length > 0 && this.buffered[length - 1] === 13)
          throw new Error("invalid worker frame");
        this.frames.push(Uint8Array.from(this.buffered));
        this.buffered = [];
        this.queuedBytes += length + 1;
      } else {
        this.buffered.push(byte);
      }
    }
  }
}

function validDiscovery(result: unknown): boolean {
  if (typeof result !== "object" || result === null) {
    return false;
  }
  const meta = (result as { _meta?: Record<string, unknown> })._meta;
  if (typeof meta !== "object" || meta === null) {
    return false;
  }
  const version = meta[PROTOCOL_META];
  const server = meta[SERVER_INFO_META];
  return (
    version === PROTOCOL_VERSION &&
    typeof server === "object" &&
    server !== null &&
    (server as { name?: unknown }).name === SERVER_NAME &&
    typeof (server as { version?: unknown }).version === "string"
  );
}

function validToolList(result: unknown): boolean {
  if (typeof result !== "object" || result === null) {
    return false;
  }
  const { tools } = result as { tools?: unknown };
  if (!Array.isArray(tools)) {
    return false;
  }
  const contextTools = tools.filter(
    (item) =>
      typeof item === "object" &&
      item !== null &&
      (item as { name?: unknown }).name === "memory_context",
  );
  if (contextTools.length !== 1) {
    return false;
  }
  const tool = contextTools[0] as {
    inputSchema?: unknown;
    outputSchema?: unknown;
  };
  return (
    equalJson(tool.inputSchema, expectedInputSchema) &&
    equalJson(tool.outputSchema, expectedOutputSchema)
  );
}

async function requestReadiness(options: {
  child: WorkerProcess;
  reader: FrameReader;
  isActive: () => boolean;
  id: string;
  method: string;
  onProtocolFailure: () => void;
}): Promise<RpcResponse> {
  const { child, reader, isActive, id, method, onProtocolFailure } = options;
  const request = {
    jsonrpc: "2.0",
    id,
    method,
    params: {
      _meta: {
        [PROTOCOL_META]: PROTOCOL_VERSION,
        [CLIENT_CAPABILITIES_META]: {},
      },
    },
  };
  const line = `${JSON.stringify(request)}\n`;
  await child.stdin.write(line);
  if (!isActive()) {
    throw new Error("startup is no longer active");
  }
  const response = await reader.next();
  if (
    response.jsonrpc !== "2.0" ||
    response.id !== id ||
    response.error !== undefined
  ) {
    onProtocolFailure();
    // A complete but invalid JSON-RPC readiness exchange is a permanent
    // protocol incompatibility, unlike a pipe/read failure.
    throw new Error("invalid readiness response");
  }
  return response;
}

/** Owns a single eager worker startup and validates its MCP identity and tool contract. */
export function createContextClient(
  config: ResolvedContextConfig,
  spawnWorker: WorkerSpawner = (options) => Bun.spawn(options),
) {
  let startup: Promise<{ readonly kind: "ready" } | typeof FAILURE> | undefined;
  let child: WorkerProcess | undefined;
  let ownedChild: WorkerProcess | undefined;
  let stderrDrain: Promise<void> | undefined;
  let frameReader: FrameReader | undefined;
  let generation = 0;
  let generationsStarted = 0;
  let permanentFailure = false;
  let generationFailed = false;
  let startupReady = false;
  let sequence = 0;
  let writeChain: Promise<void> = Promise.resolve();
  const pending = new Map<
    string,
    {
      resolve: (
        value:
          | {
              readonly kind: "context";
              readonly memories: readonly ContextMemory[];
            }
          | typeof FAILURE,
      ) => void;
    }
  >();
  const retired = new Map<number, Set<string>>();
  const pumps = new Map<number, Promise<void>>();
  const pumpReady = new Map<number, Promise<void>>();
  const resolvePumpReady = new Map<number, () => void>();
  let closing = false;
  let closePromise: Promise<void> | undefined;
  let terminationRequested = false;
  let childExited = false;
  let shutdownFailed = false;
  let cancelStartup: (() => void) | undefined;

  function failGeneration(failedGeneration = generation): void {
    if (generation !== failedGeneration) return;
    if (generationFailed) return;
    generationFailed = true;
    if (startupReady) startup = undefined;
    writeChain = Promise.resolve();
    for (const waiter of pending.values()) waiter.resolve(FAILURE);
    pending.clear();
    if (!closing && !childExited) child?.kill("SIGKILL");
  }

  const startEnabled = async (
    settings: EnabledContextSettings,
    environment: Record<string, string>,
  ): Promise<{ readonly kind: "ready" } | typeof FAILURE> => {
    let timer: ReturnType<typeof setTimeout> | undefined;
    let reader: StartupReader | undefined;
    let readerReleased = false;
    let active = true;
    let readiness: Promise<{ readonly kind: "ready" }> | undefined;
    let startedGeneration = 0;
    let startedReader: FrameReader | undefined;
    let startedChild: WorkerProcess | undefined;
    const startupDeadline = Date.now() + settings.startupTimeoutMs;
    const deadline = new Promise<typeof FAILURE>((resolve) => {
      timer = setTimeout(() => resolve(FAILURE), settings.startupTimeoutMs);
    });
    const cancelled = new Promise<typeof FAILURE>((resolve) => {
      cancelStartup = () => {
        active = false;
        resolve(FAILURE);
      };
    });
    try {
      generationsStarted += 1;
      generationFailed = false;
      startupReady = false;
      sequence = 0;
      const currentChild = spawnWorker({
        cmd: [settings.workerPath],
        env: environment,
        stdin: "pipe",
        stdout: "pipe",
        stderr: "pipe",
      });
      startedChild = currentChild;
      child = currentChild;
      ownedChild = currentChild;
      terminationRequested = false;
      childExited = false;
      let exited = false;
      generation += 1;
      const currentGeneration = generation;
      startedGeneration = currentGeneration;
      pumpReady.set(
        currentGeneration,
        new Promise<void>((resolve) =>
          resolvePumpReady.set(currentGeneration, resolve),
        ),
      );
      const exit = currentChild.exited.then(() => {
        exited = true;
        childExited = true;
        if (child === currentChild) child = undefined;
        if (shutdownFailed && ownedChild === currentChild)
          ownedChild = undefined;
        if (!generationFailed && generation === currentGeneration)
          failGeneration(currentGeneration);
        return FAILURE;
      });
      if (Date.now() >= startupDeadline) {
        child.kill("SIGKILL");
        return FAILURE;
      }
      stderrDrain = drain(child.stderr);
      reader = child.stdout.getReader();
      const currentReader = new FrameReader(reader, settings.maxFrameBytes);
      startedReader = currentReader;
      frameReader = currentReader;
      readiness = (async () => {
        if (!active) {
          throw new Error("startup is no longer active");
        }
        const discovery = await requestReadiness({
          child: currentChild,
          reader: currentReader,
          isActive: () => active,
          id: "startup-discover",
          method: "server/discover",
          onProtocolFailure: () => {
            permanentFailure = true;
          },
        });
        if (!validDiscovery(discovery.result)) {
          permanentFailure = true;
          throw new Error("invalid server discovery");
        }
        if (!active) {
          throw new Error("startup is no longer active");
        }
        const listing = await requestReadiness({
          child: currentChild,
          reader: currentReader,
          isActive: () => active,
          id: "startup-tools",
          method: "tools/list",
          onProtocolFailure: () => {
            permanentFailure = true;
          },
        });
        if (!validToolList(listing.result)) {
          permanentFailure = true;
          throw new Error("invalid tool schema");
        }
        readerReleased = true;
        startPump(currentGeneration, currentReader);
        return { kind: "ready" } as const;
      })();
      const outcome = await Promise.race([
        readiness,
        deadline,
        exit,
        cancelled,
      ]);
      if (outcome.kind === "failure") {
        active = false;
        generationFailed = true;
        if (!exited) {
          terminationRequested = true;
          currentChild.kill("SIGKILL");
        }
        return FAILURE;
      }
      startupReady = true;
      return outcome;
    } catch {
      active = false;
      generationFailed = true;
      if (child !== undefined) {
        terminationRequested = true;
        child.kill("SIGKILL");
      }
      return FAILURE;
    } finally {
      if (timer !== undefined) {
        clearTimeout(timer);
      }
      cancelStartup = undefined;
      if (reader !== undefined && !readerReleased) {
        const releaseReader = () => {
          if (!readerReleased) {
            reader?.releaseLock();
            readerReleased = true;
          }
        };
        if (readiness === undefined) {
          releaseReader();
        } else {
          readiness
            .then(
              () => {
                if (startedReader !== undefined && startedChild !== undefined)
                  startPump(startedGeneration, startedReader);
                else releaseReader();
              },
              () => {
                if (startedReader !== undefined && startedChild !== undefined)
                  startPump(startedGeneration, startedReader);
                else releaseReader();
              },
            )
            .catch(() => undefined);
        }
      }
    }
  };

  function startPump(pumpGeneration: number, pumpReader: FrameReader): void {
    if (pumps.has(pumpGeneration)) return;
    resolvePumpReady.get(pumpGeneration)?.();
    resolvePumpReady.delete(pumpGeneration);
    let pump!: Promise<void>;
    pump = (async () => {
      try {
        while (true) {
          const response = await pumpReader.next();
          if (closing || generationFailed || pumpGeneration !== generation)
            continue;
          const retiredIds = retired.get(pumpGeneration);
          if (retiredIds?.has(response.id as string)) {
            retiredIds.delete(response.id as string);
            continue;
          }
          if (
            response.jsonrpc !== "2.0" ||
            typeof response.id !== "string" ||
            !pending.has(response.id)
          )
            throw new Error("unknown protocol response");
          const waiter = pending.get(response.id) as typeof pending extends Map<
            string,
            infer V
          >
            ? V
            : never;
          pending.delete(response.id);
          const resultValue = response.result;
          const memories = (resultValue as { memories?: unknown } | undefined)
            ?.memories;
          const exactResult =
            typeof resultValue === "object" &&
            resultValue !== null &&
            !Array.isArray(resultValue) &&
            Object.keys(resultValue).length === 1 &&
            Object.hasOwn(resultValue, "memories");
          if (response.error !== undefined) waiter.resolve(FAILURE);
          else if (
            !(exactResult && Array.isArray(memories)) ||
            !memories.every((item) => validMemory(item))
          ) {
            waiter.resolve(FAILURE);
            failGeneration(pumpGeneration);
          } else
            waiter.resolve({
              kind: "context",
              memories: memories as ContextMemory[],
            });
        }
      } catch {
        if (closing) await pumpReader.discardToEof();
        else failGeneration(pumpGeneration);
      } finally {
        pumpReader.release();
        if (pumps.get(pumpGeneration) === pump) pumps.delete(pumpGeneration);
      }
    })();
    pumps.set(pumpGeneration, pump);
  }

  return {
    start(): Promise<{ readonly kind: "ready" } | typeof FAILURE> {
      if (closing) return Promise.resolve(FAILURE);
      if (startup !== undefined) {
        return startup;
      }
      if (config.mode === "enabled") {
        startup = startEnabled(config.settings, {
          ...config.childEnvironment,
        }).then((result) => {
          if (result.kind === "failure" && permanentFailure) return result;
          if (result.kind === "failure") startup = undefined;
          return result;
        });
      } else {
        startup = Promise.resolve(FAILURE);
      }
      return startup;
    },
    async request(limit: number): Promise<
      | {
          readonly kind: "context";
          readonly memories: readonly ContextMemory[];
        }
      | typeof FAILURE
    > {
      if (closing) return FAILURE;
      const lookupTimeout =
        config.mode === "enabled" ? config.settings.lookupTimeoutMs : 0;
      const deadline = Date.now() + lookupTimeout;
      if (
        config.mode !== "enabled" ||
        !Number.isSafeInteger(limit) ||
        limit < 1 ||
        limit > config.settings.limit
      )
        return FAILURE;
      if (permanentFailure || (generationsStarted >= 2 && generationFailed))
        return FAILURE;
      if (await this.start().then((result) => result.kind !== "ready"))
        return FAILURE;
      if (Date.now() >= deadline) {
        failGeneration();
        return FAILURE;
      }
      if (
        pending.size >= config.settings.maxPending ||
        frameReader === undefined ||
        child === undefined
      )
        return FAILURE;
      sequence += 1;
      const id = `${generation}:${sequence}`;
      const message = JSON.stringify({
        jsonrpc: "2.0",
        id,
        method: "tools/call",
        params: { name: "memory_context", arguments: { limit } },
      });
      const responsePromise = new Promise<
        | {
            readonly kind: "context";
            readonly memories: readonly ContextMemory[];
          }
        | typeof FAILURE
      >((resolve) => pending.set(id, { resolve }));
      const requestChild = child;
      const requestGeneration = generation;
      writeChain = writeChain.then(async () => {
        if (generationFailed || generation !== requestGeneration)
          throw new Error("worker generation failed");
        await requestChild.stdin.write(`${message}\n`);
      });
      writeChain.catch(() => failGeneration(requestGeneration));
      let timer: ReturnType<typeof setTimeout> | undefined;
      const timeout = new Promise<typeof FAILURE>((resolve) => {
        timer = setTimeout(
          () => {
            if (pending.has(id)) {
              let retiredIds = retired.get(requestGeneration);
              if (retiredIds === undefined) {
                retiredIds = new Set();
                retired.set(requestGeneration, retiredIds);
              }
              retiredIds.add(id);
              pending.delete(id);
              resolve(FAILURE);
              failGeneration();
            }
          },
          Math.max(0, deadline - Date.now()),
        );
      });
      return Promise.race([responsePromise, timeout]).finally(() => {
        if (timer !== undefined) clearTimeout(timer);
      });
    },
    /** Internal observability for lifecycle composition and focused readiness tests. */
    get process(): WorkerProcess | undefined {
      return child;
    },
    get stderrDrained(): Promise<void> | undefined {
      return stderrDrain;
    },
    close(): Promise<void> {
      if (closePromise !== undefined) return closePromise;
      closing = true;
      cancelStartup?.();
      const timeoutMs =
        config.mode === "enabled" ? config.settings.shutdownTimeoutMs : 1;
      const deadline = Date.now() + timeoutMs;
      closePromise = (async () => {
        for (const waiter of pending.values()) waiter.resolve(FAILURE);
        pending.clear();
        const within = async (promise: Promise<unknown>, ms: number) => {
          if (ms <= 0) return false;
          let timer: ReturnType<typeof setTimeout> | undefined;
          try {
            return await Promise.race([
              promise.then(
                () => true,
                () => false,
              ),
              new Promise<boolean>((resolve) => {
                timer = setTimeout(() => resolve(false), ms);
              }),
            ]);
          } finally {
            if (timer !== undefined) clearTimeout(timer);
          }
        };
        if (startup !== undefined) {
          await within(startup, deadline - Date.now());
          if (Date.now() >= deadline) {
            shutdownFailed = true;
            throw new Error("context_shutdown_failed");
          }
        }
        const current = ownedChild;
        if (current === undefined) return;
        try {
          current.stdin.end();
        } catch {
          /* stdin is already closed */
        }
        const exited = current.exited.then(
          () => true,
          () => true,
        );
        let sentTerm = false;
        if (!terminationRequested && !childExited) {
          try {
            current.kill("SIGTERM");
            terminationRequested = true;
            sentTerm = true;
          } catch {
            /* child may have exited */
          }
        }
        const stdoutDrained = (
          pumpReady.get(generation) ?? Promise.resolve()
        ).then(() => pumps.get(generation) ?? Promise.resolve());
        const drainPipes = Promise.all([
          stdoutDrained,
          stderrDrain ?? drain(current.stderr),
        ]);
        const remaining = Math.max(0, deadline - Date.now());
        const grace = Math.min(250, Math.floor(remaining / 2));
        let hasExited =
          childExited ||
          (await within(exited, sentTerm ? grace : deadline - Date.now()));
        if (!hasExited && Date.now() < deadline) {
          try {
            current.kill("SIGKILL");
          } catch {
            /* child may have exited */
          }
          hasExited = await within(exited, deadline - Date.now());
        }
        const pipesDrained = await within(drainPipes, deadline - Date.now());
        if (!hasExited || !pipesDrained) {
          shutdownFailed = true;
          if (childExited && ownedChild === current) ownedChild = undefined;
          throw new Error("context_shutdown_failed");
        }
        if (ownedChild === current) ownedChild = undefined;
        pumpReady.delete(generation);
      })();
      return closePromise;
    },
  };
}
