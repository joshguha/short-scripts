const dotenv = require("dotenv");
const { EAS, SchemaEncoder } = require("@ethereum-attestation-service/eas-sdk");
const ethers = require("ethers");
const fs = require("fs");
const csv = require("csv-parser");

// Load environment variables
dotenv.config();

// Ethereum setup
const EASContractAddress = "0xA1207F3BBa224E2c9c3c6D5aF63D0eb1582Ce587";
const privateKey = process.env.PRIVATE_KEY;
const infuraProjectId = process.env.INFURA_API_KEY;

const provider = new ethers.providers.InfuraProvider(
  "mainnet",
  infuraProjectId
);
const signer = new ethers.Wallet(privateKey, provider);

// Initialize Ethereum Attestation Service and get the Offchain class
const eas = new EAS(EASContractAddress);
const offchain = eas.getOffchain();

// Function to read Ethereum addresses from a CSV
function readCSV(filePath) {
  return new Promise((resolve) => {
    const addresses = [];
    fs.createReadStream(filePath)
      .pipe(csv())
      .on("data", (row) => addresses.push(row.address))
      .on("end", () => resolve(addresses));
  });
}

// Function to set up a write stream for successful attestations
function createWriteStreamForAttestations() {
  const writeStream = fs.createWriteStream("successful_attestations.csv");
  writeStream.write("Address,UID,Signature,Data\n");
  return writeStream;
}

async function main() {
  const addresses = await readCSV("./data/eligible_test.csv");
  const writeStream = createWriteStreamForAttestations();

  // Define schema and encode data
  const schemaEncoder = new SchemaEncoder(
    "bool StressTestParticipant, string Gyroscope"
  );
  const encodedData = schemaEncoder.encodeData([
    { name: "StressTestParticipant", value: true, type: "bool" },
    { name: "Gyroscope", value: "Gyroscope", type: "string" },
  ]);

  const schemaUID =
    "0x5831e469b10623d9220a3b8f3ae9b841c6c177039efebfd56dbc8dd314a39537";

  // Create attestations for each address
  for (const address of addresses) {
    try {
      const attestation = await offchain.signOffchainAttestation(
        {
          recipient: address,
          expirationTime: 0,
          time: Math.floor(Date.now() / 1000),
          revocable: false,
          version: 1,
          nonce: 0,
          schema: schemaUID,
          refUID:
            "0x0000000000000000000000000000000000000000000000000000000000000000",
          data: encodedData,
        },
        signer
      );

      // Write the successful attestation to the output CSV
      writeStream.write(
        `${address},${attestation.UID},${attestation.signature},${attestation.data}\n`
      );
      console.log(`Successfully created attestation for address ${address}`);
    } catch (error) {
      console.error(
        `Failed to send attestation for address ${address}. Error: ${error.message}`
      );
    }
  }

  // Close the write stream
  writeStream.end();
}

main();
