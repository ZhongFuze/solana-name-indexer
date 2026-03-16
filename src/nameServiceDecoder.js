import bs58 from "bs58";

const NS_PROGRAM_ID = "namesLPneVptA9Z5rqUDD9tMTWEJwofgaYwp8cawRkX";

const INSTRUCTION_TAGS = {
  0: "NameRegistryCreate",
  1: "NameRegistryUpdate",
  2: "NameRegistryTransfer",
  3: "NameRegistryDelete"
};

function readU32LE(buffer, offset) {
  if (offset + 4 > buffer.length) {
    throw new Error("u32 out of range");
  }
  return buffer.readUInt32LE(offset);
}

function readU64LE(buffer, offset) {
  if (offset + 8 > buffer.length) {
    throw new Error("u64 out of range");
  }
  return Number(buffer.readBigUInt64LE(offset));
}

function safeUtf8Preview(buffer) {
  if (!buffer.length) {
    return "";
  }
  const preview = buffer.toString("utf8");
  const clean = preview.replace(/\u0000/g, "");
  return /^[\x20-\x7E\n\r\t]+$/.test(clean) ? clean : "";
}

function decodeCreate(buffer) {
  let offset = 1;
  const hashedNameLen = readU32LE(buffer, offset);
  offset += 4;

  if (offset + hashedNameLen > buffer.length) {
    throw new Error("invalid hashed name length");
  }
  const hashedName = buffer.subarray(offset, offset + hashedNameLen);
  offset += hashedNameLen;

  const lamports = readU64LE(buffer, offset);
  offset += 8;

  const space = readU32LE(buffer, offset);
  offset += 4;

  return {
    instructionName: INSTRUCTION_TAGS[0],
    decoded: {
      hashedNameHex: hashedName.toString("hex"),
      hashedNameBase58: bs58.encode(hashedName),
      lamports,
      space,
      remainingBytes: buffer.length - offset
    }
  };
}

function decodeUpdate(buffer) {
  let offset = 1;
  const dataOffset = readU32LE(buffer, offset);
  offset += 4;

  const dataLen = readU32LE(buffer, offset);
  offset += 4;

  if (offset + dataLen > buffer.length) {
    throw new Error("invalid update data length");
  }

  const updateData = buffer.subarray(offset, offset + dataLen);
  offset += dataLen;

  return {
    instructionName: INSTRUCTION_TAGS[1],
    decoded: {
      offset: dataOffset,
      dataLen,
      dataHex: updateData.toString("hex"),
      utf8Preview: safeUtf8Preview(updateData),
      remainingBytes: buffer.length - offset
    }
  };
}

function decodeTransfer(buffer, accountKeys = [], accountIndexes = []) {
  const newOwnerIndex = accountIndexes[2];
  const newOwner = typeof newOwnerIndex === "number" ? accountKeys[newOwnerIndex] : null;
  return {
    instructionName: INSTRUCTION_TAGS[2],
    decoded: {
      rawDataHex: buffer.toString("hex"),
      inferredNewOwner: newOwner ?? null
    }
  };
}

function decodeDelete(buffer) {
  return {
    instructionName: INSTRUCTION_TAGS[3],
    decoded: {
      rawDataHex: buffer.toString("hex")
    }
  };
}

export function decodeNameServiceInstruction(encodedData, accountKeys = [], accountIndexes = []) {
  if (!encodedData || typeof encodedData !== "string") {
    return {
      matched: false,
      reason: "missing instruction data"
    };
  }

  let raw;
  try {
    raw = Buffer.from(bs58.decode(encodedData));
  } catch (error) {
    return {
      matched: false,
      reason: `base58 decode failed: ${error.message}`
    };
  }

  if (!raw.length) {
    return {
      matched: false,
      reason: "empty instruction data"
    };
  }

  const tag = raw[0];

  try {
    if (tag === 0) {
      return {
        matched: true,
        tag,
        ...decodeCreate(raw)
      };
    }
    if (tag === 1) {
      return {
        matched: true,
        tag,
        ...decodeUpdate(raw)
      };
    }
    if (tag === 2) {
      return {
        matched: true,
        tag,
        ...decodeTransfer(raw, accountKeys, accountIndexes)
      };
    }
    if (tag === 3) {
      return {
        matched: true,
        tag,
        ...decodeDelete(raw)
      };
    }

    return {
      matched: true,
      tag,
      instructionName: `Unknown(${tag})`,
      decoded: {
        rawDataHex: raw.toString("hex"),
        rawDataLen: raw.length
      }
    };
  } catch (error) {
    return {
      matched: true,
      tag,
      instructionName: INSTRUCTION_TAGS[tag] ?? `Unknown(${tag})`,
      decodeError: error.message,
      decoded: {
        rawDataHex: raw.toString("hex"),
        rawDataLen: raw.length
      }
    };
  }
}

export function isNameServiceProgram(programId) {
  return programId === NS_PROGRAM_ID;
}

export { NS_PROGRAM_ID, INSTRUCTION_TAGS };
