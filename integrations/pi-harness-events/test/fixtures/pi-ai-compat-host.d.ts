declare module "@earendil-works/pi-ai/compat" {
  type FixtureProvider = Parameters<
    import("@earendil-works/pi-coding-agent").ModelRuntime["registerNativeProvider"]
  >[0];
  type FixtureEventStream = ReturnType<FixtureProvider["streamSimple"]>;
  type FixtureToolParameters = Parameters<
    import("@earendil-works/pi-coding-agent").ExtensionAPI["registerTool"]
  >[0]["parameters"];

  export function createProvider(input: unknown): FixtureProvider;
  export function createAssistantMessageEventStream(): FixtureEventStream;
  export const Type: {
    Object(properties: Record<string, never>): FixtureToolParameters;
  };
}
