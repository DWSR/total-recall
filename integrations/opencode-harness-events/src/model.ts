export type JsonPrimitive = string | number | boolean | null;

export type JsonValue = JsonPrimitive | JsonObject | readonly JsonValue[];

export interface JsonObject {
  readonly [key: string]: JsonValue;
}

export type Generation = "v1" | "v2";

export interface EventMetadata {
  readonly generation: Generation;
  readonly sessionId: string;
  readonly projectName: string;
  readonly currentWorkingDirectory: string;
  readonly timestamp: string;
}

export type Submission =
  | { readonly kind: "start"; readonly metadata: EventMetadata }
  | {
      readonly kind: "observation";
      readonly metadata: EventMetadata;
      readonly hookType: string;
      readonly data: JsonObject;
    }
  | { readonly kind: "end"; readonly metadata: EventMetadata };

export type DispatchResult =
  | { readonly ok: true }
  | {
      readonly ok: false;
      readonly category: DispatchFailureCategory;
    };

export type DispatchFailureCategory =
  | "configuration"
  | "serialization"
  | "spawn"
  | "timeout"
  | "usage"
  | "connection"
  | "invocation"
  | "signal";

export interface Diagnostic {
  readonly category: DispatchFailureCategory;
  readonly generation?: Generation;
  readonly nativeKind?: string;
  readonly sessionId?: string;
}
