const dotenv = require('dotenv');
const result = dotenv.config({ path: '/Users/jonas/Workspace/local/attestations/.env' });

if (result.error) {
  throw result.error;
}

const { EAS, SchemaEncoder, SchemaRegistry } = require("@ethereum-attestation-service/eas-sdk");
const ethers = require('ethers');
const fs = require('fs');
const csv = require('csv-parser');

// configs & init
const EASContractAddress = "0xC2679fBD37d54388Ce493F1DB75320D236e1815e";
const SCHEMA_REGISTRY_ADDRESS = "0xA7b39296258348C78294F95B872b282326A97BDF";
const privateKey = process.env.PRIVATE_KEY;
const infuraProjectId = process.env.INFURA_API_KEY;
// const contractABI = require('/Users/jonas/Workspace/local/attestations/abi.json');


const provider = new ethers.providers.InfuraProvider("mainnet", infuraProjectId);
const signer = new ethers.Wallet(privateKey, provider);

// CSV part
function readCSV(filePath) {
  return new Promise((resolve, reject) => {
    const addresses = [];
    fs.createReadStream(filePath)
      .pipe(csv())
      .on('data', (row) => addresses.push(row.address))
      .on('end', () => {
        resolve(addresses);
      });
  });
}

function createWriteStreamForAttestations() {
  const writeStream = fs.createWriteStream('completed_attestations_test.csv');
  writeStream.write('Address,UID,Signature,AttestationData\n');
  return writeStream;
}

// interact w/ EAS
const eas = new EAS(EASContractAddress);
eas.connect(signer);

async function sendAttestation(address) {
  const offchain = await eas.getOffchain();
  const schemaEncoder = new SchemaEncoder("bool StressTestParticipant, string Gyroscope");
  const encodedData = schemaEncoder.encodeData([
    { name: "StressTestParticipant", value: true, type: "bool" },
    { name: "Gyroscope", value: "Gyroscope", type: "string" },
  ]);
  
  const offchainAttestation = await offchain.signOffchainAttestation({
      recipient: address,
      expirationTime: 0,
      time: Math.floor(Date.now() / 1000),  
      revocable: false,
      version: 1,
      nonce: 0,
      schema: "0x5831e469b10623d9220a3b8f3ae9b841c6c177039efebfd56dbc8dd314a39537",
      refUID: '0x0000000000000000000000000000000000000000000000000000000000000000',
      data: encodedData,
  }, signer);

  return offchainAttestation;
}

async function getSchemaInfo(schemaUID) {
  const schemaRegistry = new SchemaRegistry(SCHEMA_REGISTRY_ADDRESS);
  schemaRegistry.connect(provider);

  try {
    const schemaRecord = await schemaRegistry.getSchema({ uid: schemaUID });
    console.log(schemaRecord);
    return schemaRecord;
  } catch (error) {
    console.error(`Failed to retrieve schema for UID ${schemaUID}. Error: ${error.message}`);
    throw error;
  }
}

// Main Logic
async function main() {
  // console.log("Fetching schema information...");
  // await getSchemaInfo("0x5831e469b10623d9220a3b8f3ae9b841c6c177039efebfd56dbc8dd314a39537");
  
  const addresses = await readCSV('/Users/jonas/Workspace/local/attestations/eligible_test.csv');
  const writeStream = createWriteStreamForAttestations();

  for (const address of addresses) {
    try {
      const attestation = await sendAttestation(address);
      writeStream.write(`${address},${attestation.UID},${attestation.signature},${attestation.data}\n`);
      console.log(`Successfully created attestation for address ${address}. UID: ${attestation.UID}`);
    } catch (error) {
      console.error(`Failed to send attestation for address ${address}. Error: ${error.message}`);
    }
  }

  writeStream.end();
}

main();
