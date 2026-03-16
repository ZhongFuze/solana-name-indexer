const ROOT_PARENT = "58PwtjSDuFHuUkYjH9BYnnQKHfwo9reZhC2zMJv9JPkx";

function addEntry(target, instructionName, nameAccount) {
  if (
    typeof instructionName === "string" &&
    instructionName.length > 0 &&
    typeof nameAccount === "string" &&
    nameAccount.length > 0
  ) {
    target.add(`${instructionName}\t${nameAccount}`);
  }
}

function extractFromInstruction(instruction, target) {
  const instructionName = instruction?.nameService?.instructionName;
  const accounts = Array.isArray(instruction?.accounts) ? instruction.accounts : [];

  if (instructionName === "NameRegistryCreate") {
    addEntry(target, instructionName, accounts[2]);
    if (accounts[5] && accounts[5] !== ROOT_PARENT) {
      addEntry(target, instructionName, accounts[5]);
    }
    return;
  }

  if (
    instructionName === "NameRegistryUpdate" ||
    instructionName === "NameRegistryTransfer" ||
    instructionName === "NameRegistryDelete"
  ) {
    addEntry(target, instructionName, accounts[0]);
  }
}

export function extractNameAccountEntries(row) {
  const signature = row?.signature;
  const instructions = Array.isArray(row?.matchedInstructions) ? row.matchedInstructions : [];
  if (typeof signature !== "string" || instructions.length === 0) {
    return [];
  }

  const uniqueEntries = new Set();
  for (const instruction of instructions) {
    extractFromInstruction(instruction, uniqueEntries);
  }

  return [...uniqueEntries].map((entry) => {
    const [instructionName, nameAccount] = entry.split("\t");
    return { signature, instructionName, nameAccount };
  });
}

export function formatTimestampForFilename(value) {
  const iso = typeof value === "string" ? value : new Date(value).toISOString();
  return iso.replace(/[:-]/g, "").replace(/\.\d{3}Z$/, "Z");
}
