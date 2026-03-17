# durable.lens/v1 JSON Schema (reference)

This file exists so the coding agent can implement parsing and validation for the lens spec without needing access to non-markdown assets.

Source: `pkg/durable-lens/schemas/durable-lens-v1.schema.json` in the original repo.

The Rust rewrite can either:
- embed this schema as a static string and validate lens JSON against it in tests, or
- treat it as documentation and implement validation rules directly.

---

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "https://example.com/schemas/durable-lens-v1.schema.json",
  "title": "Durable Stream Lens Spec",
  "type": "object",
  "additionalProperties": false,
  "required": ["apiVersion", "schema", "from", "to", "ops"],
  "properties": {
    "apiVersion": {
      "type": "string",
      "const": "durable.lens/v1"
    },
    "schema": {
      "type": "string",
      "minLength": 1,
      "description": "Logical stream schema/type name (e.g., 'Task')."
    },
    "from": {
      "type": "integer",
      "minimum": 0,
      "description": "Source schema version."
    },
    "to": {
      "type": "integer",
      "minimum": 0,
      "description": "Target schema version."
    },
    "description": {
      "type": "string"
    },
    "ops": {
      "type": "array",
      "minItems": 1,
      "items": { "$ref": "#/$defs/op" }
    }
  },
  "$defs": {
    "jsonPointer": {
      "type": "string",
      "description": "RFC 6901 JSON Pointer. Empty string refers to the document root.",
      "pattern": "^(?:/(?:[^~/]|~0|~1)*)*$"
    },
    "jsonScalar": {
      "description": "JSON scalar value.",
      "type": ["string", "number", "integer", "boolean", "null"]
    },
    "embeddedJsonSchema": {
      "description": "An embedded JSON Schema fragment (object or boolean).",
      "anyOf": [
        { "type": "object" },
        { "type": "boolean" }
      ]
    },

    "mapTransform": {
      "type": "object",
      "additionalProperties": false,
      "required": ["map"],
      "properties": {
        "map": {
          "type": "object",
          "description": "Mapping table for values. Keys must be strings; values are JSON scalars.",
          "additionalProperties": { "$ref": "#/$defs/jsonScalar" }
        },
        "default": {
          "$ref": "#/$defs/jsonScalar",
          "description": "Default output value used when input is not found in map."
        }
      }
    },
    "builtinTransform": {
      "type": "object",
      "additionalProperties": false,
      "required": ["builtin"],
      "properties": {
        "builtin": {
          "type": "string",
          "minLength": 1,
          "description": "Name of a built-in, version-stable transform implemented by the system."
        }
      }
    },
    "convertTransform": {
      "description": "Restricted conversion: either a total mapping table (+ optional default) or a built-in transform.",
      "oneOf": [
        { "$ref": "#/$defs/mapTransform" },
        { "$ref": "#/$defs/builtinTransform" }
      ]
    },

    "opRename": {
      "type": "object",
      "additionalProperties": false,
      "required": ["op", "from", "to"],
      "properties": {
        "op": { "const": "rename" },
        "from": { "$ref": "#/$defs/jsonPointer" },
        "to": { "$ref": "#/$defs/jsonPointer" }
      }
    },
    "opCopy": {
      "type": "object",
      "additionalProperties": false,
      "required": ["op", "from", "to"],
      "properties": {
        "op": { "const": "copy" },
        "from": { "$ref": "#/$defs/jsonPointer" },
        "to": { "$ref": "#/$defs/jsonPointer" }
      }
    },
    "opAdd": {
      "type": "object",
      "additionalProperties": false,
      "required": ["op", "path", "schema"],
      "properties": {
        "op": { "const": "add" },
        "path": { "$ref": "#/$defs/jsonPointer" },
        "schema": { "$ref": "#/$defs/embeddedJsonSchema" },
        "default": {
          "description": "Default value to insert if the field is missing. If omitted, the runtime may derive a default when possible.",
          "type": ["object", "array", "string", "number", "integer", "boolean", "null"]
        }
      }
    },
    "opRemove": {
      "type": "object",
      "additionalProperties": false,
      "required": ["op", "path", "schema"],
      "properties": {
        "op": { "const": "remove" },
        "path": { "$ref": "#/$defs/jsonPointer" },
        "schema": {
          "$ref": "#/$defs/embeddedJsonSchema",
          "description": "Schema of the removed field (required so the transformation remains declarative and reversible/validatable)."
        },
        "default": {
          "description": "Default to use when reconstructing the field (e.g., during backward reasoning/validation).",
          "type": ["object", "array", "string", "number", "integer", "boolean", "null"]
        }
      }
    },

    "opHoist": {
      "type": "object",
      "additionalProperties": false,
      "required": ["op", "host", "name", "to"],
      "properties": {
        "op": { "const": "hoist" },
        "host": {
          "$ref": "#/$defs/jsonPointer",
          "description": "Pointer to an object field that contains the nested value."
        },
        "name": {
          "type": "string",
          "minLength": 1,
          "description": "Field name inside host to move outward."
        },
        "to": {
          "$ref": "#/$defs/jsonPointer",
          "description": "Destination pointer for the hoisted value."
        },
        "removeFromHost": {
          "type": "boolean",
          "default": true,
          "description": "If true, remove the nested field from the host after hoisting."
        }
      }
    },
    "opPlunge": {
      "type": "object",
      "additionalProperties": false,
      "required": ["op", "from", "host", "name"],
      "properties": {
        "op": { "const": "plunge" },
        "from": {
          "$ref": "#/$defs/jsonPointer",
          "description": "Pointer to the source field to move inward."
        },
        "host": {
          "$ref": "#/$defs/jsonPointer",
          "description": "Pointer to the destination object field."
        },
        "name": {
          "type": "string",
          "minLength": 1,
          "description": "Field name inside host to receive the value."
        },
        "createHost": {
          "type": "boolean",
          "default": true,
          "description": "If true, create the destination host object if missing."
        },
        "removeFromSource": {
          "type": "boolean",
          "default": true,
          "description": "If true, remove the source field after plunging."
        }
      }
    },

    "opWrap": {
      "type": "object",
      "additionalProperties": false,
      "required": ["op", "path", "mode"],
      "properties": {
        "op": { "const": "wrap" },
        "path": { "$ref": "#/$defs/jsonPointer" },
        "mode": {
          "type": "string",
          "enum": ["singleton"],
          "description": "singleton: x -> [x]"
        },
        "reverseMode": {
          "type": "string",
          "enum": ["first"],
          "default": "first",
          "description": "When reversing array->scalar, choose 'first'."
        }
      }
    },
    "opHead": {
      "type": "object",
      "additionalProperties": false,
      "required": ["op", "path"],
      "properties": {
        "op": { "const": "head" },
        "path": { "$ref": "#/$defs/jsonPointer" },
        "reverseMode": {
          "type": "string",
          "enum": ["singleton"],
          "default": "singleton",
          "description": "When reversing scalar->array, wrap as [scalar]."
        }
      }
    },

    "opConvert": {
      "type": "object",
      "additionalProperties": false,
      "required": ["op", "path", "fromType", "toType", "forward", "backward"],
      "properties": {
        "op": { "const": "convert" },
        "path": { "$ref": "#/$defs/jsonPointer" },
        "fromType": {
          "type": "string",
          "enum": ["string", "number", "integer", "boolean", "null", "object", "array"]
        },
        "toType": {
          "type": "string",
          "enum": ["string", "number", "integer", "boolean", "null", "object", "array"]
        },
        "forward": { "$ref": "#/$defs/convertTransform" },
        "backward": { "$ref": "#/$defs/convertTransform" }
      }
    },

    "opIn": {
      "type": "object",
      "additionalProperties": false,
      "required": ["op", "path", "ops"],
      "properties": {
        "op": { "const": "in" },
        "path": { "$ref": "#/$defs/jsonPointer" },
        "ops": {
          "type": "array",
          "minItems": 1,
          "items": { "$ref": "#/$defs/op" }
        }
      }
    },
    "opMap": {
      "type": "object",
      "additionalProperties": false,
      "required": ["op", "path", "ops"],
      "properties": {
        "op": { "const": "map" },
        "path": { "$ref": "#/$defs/jsonPointer" },
        "ops": {
          "type": "array",
          "minItems": 1,
          "items": { "$ref": "#/$defs/op" }
        }
      }
    },

    "op": {
      "description": "A single lens operation.",
      "oneOf": [
        { "$ref": "#/$defs/opRename" },
        { "$ref": "#/$defs/opCopy" },
        { "$ref": "#/$defs/opAdd" },
        { "$ref": "#/$defs/opRemove" },
        { "$ref": "#/$defs/opHoist" },
        { "$ref": "#/$defs/opPlunge" },
        { "$ref": "#/$defs/opWrap" },
        { "$ref": "#/$defs/opHead" },
        { "$ref": "#/$defs/opConvert" },
        { "$ref": "#/$defs/opIn" },
        { "$ref": "#/$defs/opMap" }
      ]
    }
  }
}
```

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "https://example.com/schemas/durable-lens-v1.schema.json",
  "title": "Durable Stream Lens Spec",
  "type": "object",
  "additionalProperties": false,
  "required": ["apiVersion", "schema", "from", "to", "ops"],
  "properties": {
    "apiVersion": {
      "type": "string",
      "const": "durable.lens/v1"
    },
    "schema": {
      "type": "string",
      "minLength": 1,
      "description": "Logical stream schema/type name (e.g., 'Task')."
    },
    "from": {
      "type": "integer",
      "minimum": 0,
      "description": "Source schema version."
    },
    "to": {
      "type": "integer",
      "minimum": 0,
      "description": "Target schema version."
    },
    "description": {
      "type": "string"
    },
    "ops": {
      "type": "array",
      "minItems": 1,
      "items": { "$ref": "#/$defs/op" }
    }
  },
  "$defs": {
    "jsonPointer": {
      "type": "string",
      "description": "RFC 6901 JSON Pointer. Empty string refers to the document root.",
      "pattern": "^(?:/(?:[^~/]|~0|~1)*)*$"
    },
    "jsonScalar": {
      "description": "JSON scalar value.",
      "type": ["string", "number", "integer", "boolean", "null"]
    },
    "embeddedJsonSchema": {
      "description": "An embedded JSON Schema fragment (object or boolean).",
      "anyOf": [
        { "type": "object" },
        { "type": "boolean" }
      ]
    },

    "mapTransform": {
      "type": "object",
      "additionalProperties": false,
      "required": ["map"],
      "properties": {
        "map": {
          "type": "object",
          "description": "Mapping table for values. Keys must be strings; values are JSON scalars.",
          "additionalProperties": { "$ref": "#/$defs/jsonScalar" }
        },
        "default": {
          "$ref": "#/$defs/jsonScalar",
          "description": "Default output value used when input is not found in map."
        }
      }
    },
    "builtinTransform": {
      "type": "object",
      "additionalProperties": false,
      "required": ["builtin"],
      "properties": {
        "builtin": {
          "type": "string",
          "minLength": 1,
          "description": "Name of a built-in, version-stable transform implemented by the system."
        }
      }
    },
    "convertTransform": {
      "description": "Restricted conversion: either a total mapping table (+ optional default) or a built-in transform.",
      "oneOf": [
        { "$ref": "#/$defs/mapTransform" },
        { "$ref": "#/$defs/builtinTransform" }
      ]
    },

    "opRename": {
      "type": "object",
      "additionalProperties": false,
      "required": ["op", "from", "to"],
      "properties": {
        "op": { "const": "rename" },
        "from": { "$ref": "#/$defs/jsonPointer" },
        "to": { "$ref": "#/$defs/jsonPointer" }
      }
    },
    "opCopy": {
      "type": "object",
      "additionalProperties": false,
      "required": ["op", "from", "to"],
      "properties": {
        "op": { "const": "copy" },
        "from": { "$ref": "#/$defs/jsonPointer" },
        "to": { "$ref": "#/$defs/jsonPointer" }
      }
    },
    "opAdd": {
      "type": "object",
      "additionalProperties": false,
      "required": ["op", "path", "schema"],
      "properties": {
        "op": { "const": "add" },
        "path": { "$ref": "#/$defs/jsonPointer" },
        "schema": { "$ref": "#/$defs/embeddedJsonSchema" },
        "default": {
          "description": "Default value to insert if the field is missing. If omitted, the runtime may derive a default when possible.",
          "type": ["object", "array", "string", "number", "integer", "boolean", "null"]
        }
      }
    },
    "opRemove": {
      "type": "object",
      "additionalProperties": false,
      "required": ["op", "path", "schema"],
      "properties": {
        "op": { "const": "remove" },
        "path": { "$ref": "#/$defs/jsonPointer" },
        "schema": {
          "$ref": "#/$defs/embeddedJsonSchema",
          "description": "Schema of the removed field (required so the transformation remains declarative and reversible/validatable)."
        },
        "default": {
          "description": "Default to use when reconstructing the field (e.g., during backward reasoning/validation).",
          "type": ["object", "array", "string", "number", "integer", "boolean", "null"]
        }
      }
    },

    "opHoist": {
      "type": "object",
      "additionalProperties": false,
      "required": ["op", "host", "name", "to"],
      "properties": {
        "op": { "const": "hoist" },
        "host": {
          "$ref": "#/$defs/jsonPointer",
          "description": "Pointer to an object field that contains the nested value."
        },
        "name": {
          "type": "string",
          "minLength": 1,
          "description": "Field name inside host to move outward."
        },
        "to": {
          "$ref": "#/$defs/jsonPointer",
          "description": "Destination pointer for the hoisted value."
        },
        "removeFromHost": {
          "type": "boolean",
          "default": true,
          "description": "If true, remove the nested field from the host after hoisting."
        }
      }
    },
    "opPlunge": {
      "type": "object",
      "additionalProperties": false,
      "required": ["op", "from", "host", "name"],
      "properties": {
        "op": { "const": "plunge" },
        "from": {
          "$ref": "#/$defs/jsonPointer",
          "description": "Pointer to the source field to move inward."
        },
        "host": {
          "$ref": "#/$defs/jsonPointer",
          "description": "Pointer to the destination object field."
        },
        "name": {
          "type": "string",
          "minLength": 1,
          "description": "Field name inside host to receive the value."
        },
        "createHost": {
          "type": "boolean",
          "default": true,
          "description": "If true, create the destination host object if missing."
        },
        "removeFromSource": {
          "type": "boolean",
          "default": true,
          "description": "If true, remove the source field after plunging."
        }
      }
    },

    "opWrap": {
      "type": "object",
      "additionalProperties": false,
      "required": ["op", "path", "mode"],
      "properties": {
        "op": { "const": "wrap" },
        "path": { "$ref": "#/$defs/jsonPointer" },
        "mode": {
          "type": "string",
          "enum": ["singleton"],
          "description": "singleton: x -> [x]"
        },
        "reverseMode": {
          "type": "string",
          "enum": ["first"],
          "default": "first",
          "description": "When reversing array->scalar, choose 'first'."
        }
      }
    },
    "opHead": {
      "type": "object",
      "additionalProperties": false,
      "required": ["op", "path"],
      "properties": {
        "op": { "const": "head" },
        "path": { "$ref": "#/$defs/jsonPointer" },
        "reverseMode": {
          "type": "string",
          "enum": ["singleton"],
          "default": "singleton",
          "description": "When reversing scalar->array, wrap as [scalar]."
        }
      }
    },

    "opConvert": {
      "type": "object",
      "additionalProperties": false,
      "required": ["op", "path", "fromType", "toType", "forward", "backward"],
      "properties": {
        "op": { "const": "convert" },
        "path": { "$ref": "#/$defs/jsonPointer" },
        "fromType": {
          "type": "string",
          "enum": ["string", "number", "integer", "boolean", "null", "object", "array"]
        },
        "toType": {
          "type": "string",
          "enum": ["string", "number", "integer", "boolean", "null", "object", "array"]
        },
        "forward": { "$ref": "#/$defs/convertTransform" },
        "backward": { "$ref": "#/$defs/convertTransform" }
      }
    },

    "opIn": {
      "type": "object",
      "additionalProperties": false,
      "required": ["op", "path", "ops"],
      "properties": {
        "op": { "const": "in" },
        "path": { "$ref": "#/$defs/jsonPointer" },
        "ops": {
          "type": "array",
          "minItems": 1,
          "items": { "$ref": "#/$defs/op" }
        }
      }
    },
    "opMap": {
      "type": "object",
      "additionalProperties": false,
      "required": ["op", "path", "ops"],
      "properties": {
        "op": { "const": "map" },
        "path": { "$ref": "#/$defs/jsonPointer" },
        "ops": {
          "type": "array",
          "minItems": 1,
          "items": { "$ref": "#/$defs/op" }
        }
      }
    },

    "op": {
      "description": "A single lens operation.",
      "oneOf": [
        { "$ref": "#/$defs/opRename" },
        { "$ref": "#/$defs/opCopy" },
        { "$ref": "#/$defs/opAdd" },
        { "$ref": "#/$defs/opRemove" },
        { "$ref": "#/$defs/opHoist" },
        { "$ref": "#/$defs/opPlunge" },
        { "$ref": "#/$defs/opWrap" },
        { "$ref": "#/$defs/opHead" },
        { "$ref": "#/$defs/opConvert" },
        { "$ref": "#/$defs/opIn" },
        { "$ref": "#/$defs/opMap" }
      ]
    }
  }
}

```
