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
  } catch {
    // A closed pipe is already drained for privacy and backpressure purposes.
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
}): Promise<RpcResponse> {
  const { child, reader, isActive, id, method } = options;
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
  let stderrDrain: Promise<void> | undefined;
  let frameReader: FrameReader | undefined;
  let generation = 0;
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
  let pump: Promise<void> | undefined;

  const startEnabled = async (
    settings: EnabledContextSettings,
    environment: Record<string, string>,
  ): Promise<{ readonly kind: "ready" } | typeof FAILURE> => {
    let timer: ReturnType<typeof setTimeout> | undefined;
    let reader: StartupReader | undefined;
    let readerReleased = false;
    let active = true;
    let readiness: Promise<{ readonly kind: "ready" }> | undefined;
    const startupDeadline = Date.now() + settings.startupTimeoutMs;
    const deadline = new Promise<typeof FAILURE>((resolve) => {
      timer = setTimeout(() => resolve(FAILURE), settings.startupTimeoutMs);
    });
    try {
      child = spawnWorker({
        cmd: [settings.workerPath],
        env: environment,
        stdin: "pipe",
        stdout: "pipe",
        stderr: "pipe",
      });
      if (Date.now() >= startupDeadline) {
        child.kill("SIGKILL");
        return FAILURE;
      }
      stderrDrain = drain(child.stderr);
      reader = child.stdout.getReader();
      generation += 1;
      frameReader = new FrameReader(reader, settings.maxFrameBytes);
      readiness = (async () => {
        if (!active) {
          throw new Error("startup is no longer active");
        }
        const discovery = await requestReadiness({
          child: child as WorkerProcess,
          reader: frameReader as FrameReader,
          isActive: () => active,
          id: "startup-discover",
          method: "server/discover",
        });
        if (!validDiscovery(discovery.result)) {
          throw new Error("invalid server discovery");
        }
        if (!active) {
          throw new Error("startup is no longer active");
        }
        const listing = await requestReadiness({
          child: child as WorkerProcess,
          reader: frameReader as FrameReader,
          isActive: () => active,
          id: "startup-tools",
          method: "tools/list",
        });
        if (!validToolList(listing.result)) {
          throw new Error("invalid tool schema");
        }
        readerReleased = true;
        return { kind: "ready" } as const;
      })();
      const outcome = await Promise.race([readiness, deadline]);
      if (outcome.kind === "failure") {
        active = false;
        child.kill("SIGKILL");
        return FAILURE;
      }
      return outcome;
    } catch {
      active = false;
      child?.kill("SIGKILL");
      return FAILURE;
    } finally {
      if (timer !== undefined) {
        clearTimeout(timer);
      }
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
          readiness.then(releaseReader, releaseReader).catch(() => undefined);
        }
      }
    }
  };

  return {
    start(): Promise<{ readonly kind: "ready" } | typeof FAILURE> {
      if (startup !== undefined) {
        return startup;
      }
      if (config.mode === "enabled") {
        startup = startEnabled(config.settings, { ...config.childEnvironment });
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
      if (
        config.mode !== "enabled" ||
        !Number.isSafeInteger(limit) ||
        limit < 1 ||
        limit > config.settings.limit
      )
        return FAILURE;
      if (await this.start().then((result) => result.kind !== "ready"))
        return FAILURE;
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
      writeChain = writeChain.then(async () => {
        await child?.stdin.write(`${message}\n`);
      });
      writeChain.catch(() => {
        for (const waiter of pending.values()) waiter.resolve(FAILURE);
        pending.clear();
      });
      if (pump === undefined) {
        pump = (async () => {
          try {
            while (pending.size > 0) {
              const response = await (frameReader as FrameReader).next();
              if (
                response.jsonrpc !== "2.0" ||
                typeof response.id !== "string" ||
                !pending.has(response.id)
              )
                throw new Error("unknown protocol response");
              const waiter = pending.get(
                response.id,
              ) as typeof pending extends Map<string, infer V> ? V : never;
              pending.delete(response.id);
              const memories = (
                response.result as { memories?: unknown } | undefined
              )?.memories;
              const resultValue = response.result;
              const exactResult =
                typeof resultValue === "object" &&
                resultValue !== null &&
                !Array.isArray(resultValue) &&
                Object.keys(resultValue).length === 1 &&
                Object.hasOwn(resultValue, "memories");
              if (
                response.error !== undefined ||
                !exactResult ||
                !Array.isArray(memories) ||
                !memories.every((item) => validMemory(item))
              )
                waiter.resolve(FAILURE);
              else
                waiter.resolve({
                  kind: "context",
                  memories: memories as ContextMemory[],
                });
            }
          } catch {
            for (const waiter of pending.values()) waiter.resolve(FAILURE);
            pending.clear();
            child?.kill("SIGKILL");
          } finally {
            pump = undefined;
          }
        })();
      }
      return responsePromise;
    },
    /** Internal observability for lifecycle composition and focused readiness tests. */
    get process(): WorkerProcess | undefined {
      return child;
    },
    get stderrDrained(): Promise<void> | undefined {
      return stderrDrain;
    },
  };
}
