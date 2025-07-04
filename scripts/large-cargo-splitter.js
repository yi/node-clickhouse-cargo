#!/usr/bin/env node
/*
 * This is a JavaScript script executed by NodeJS.
 * Upon initialisation, this script accepts two parameters from the command line:
 *  1. pathToSourceFile: the path to the source file to be processed
 *  2. outputFolder: the destination path for output files
 * These two parameters must be carefully validated before proceeding.
 *
 * The script reads the source file line by line from `pathToSourceFile`. Each line is parsed using JSON.parse.
 * Any invalid lines are logged to the console and subsequently ignored.
 * The result of JSON.parse must be a non-empty array.
 *
 * The script saves arrays in batches of 100 to the `outputFolder`, with one JSON.stringify(array) per line.
 *
 * The output filename follows the format: `${input file name}.${Date.now().toString(36)}_${++CountOfOutPutFile}.0.uncommitted`
 *
 **/

const fs = require('fs');
const path = require('path');

// Get command line arguments
const args = process.argv.slice(2);

// Check if we have the required parameters
if (args.length !== 2) {
    console.error('Usage: node large-cargo-splitter.js <pathToSourceFile> <outputFolder>');
    process.exit(1);
}

const pathToSourceFile = args[0];
const outputFolder = args[1];

// Validate input file path
if (!fs.existsSync(pathToSourceFile)) {
    console.error(`Error: Source file '${pathToSourceFile}' does not exist`);
    process.exit(1);
}
// Validate output folder
if (!fs.existsSync(outputFolder)) {
    console.error(`Error: Output folder '${outputFolder}' does not exist`);
    process.exit(1);
}

// Get input file name for output file naming
const inputFileName = path.basename(pathToSourceFile);
const timestamp = Date.now().toString(36);
let outputFileCount = 0;

// Initialize variables for processing
let currentBatch = [];
const batchSize = 100;

// Function to write batch to file
function writeBatchToFile(batch, batchNumber) {
    const outputFileName = `${inputFileName}.${timestamp}_${batchNumber}.0.uncommitted`;
    const outputFilePath = path.join(outputFolder, outputFileName);

    try {
        const outputContent = batch.map(array => JSON.stringify(array)).join('\n');
        fs.writeFileSync(outputFilePath, outputContent);
        console.log(`Written batch ${batchNumber} to: ${outputFilePath}`);
    } catch (error) {
        console.error(`Error writing batch ${batchNumber} to file:`, error.message);
    }
}

// Read and process the source file line by line using streams
try {
    const readline = require('readline');
    const fileStream = fs.createReadStream(pathToSourceFile, {
        encoding: 'utf8',
        highWaterMark: 64 * 1024 // 64KB chunks
    });

    const rl = readline.createInterface({
        input: fileStream,
        crlfDelay: Infinity
    });

    let lineNumber = 0;

    rl.on('line', (line) => {
        lineNumber++;
        line = line.trim();

        // Skip empty lines
        if (!line) {
            return;
        }

        try {
            // Parse the line as JSON
            const parsedData = JSON.parse(line);

            // Check if the result is a non-empty array
            if (!Array.isArray(parsedData) || parsedData.length === 0) {
                console.log(`❌ Line ${lineNumber}: Invalid data - not a non-empty array`);
                return;
            }

            // Add to current batch
            currentBatch.push(parsedData);

            // Check if batch is full
            if (currentBatch.length >= batchSize) {
                outputFileCount++;
                writeBatchToFile(currentBatch, outputFileCount);
                currentBatch = [];
            }

        } catch (parseError) {
            console.log(`❌ Line ${lineNumber}: Invalid JSON - ${parseError.message}`);
            return;
        }
    });

    rl.on('error', (error) => {
        console.error('❌ Error reading source file:', error.message);
        process.exit(1);
    });

    rl.on('close', () => {
        // Write remaining items in the last batch
        if (currentBatch.length > 0) {
            outputFileCount++;
            writeBatchToFile(currentBatch, outputFileCount);
        }
        console.log(`✅ Processing complete. Created ${outputFileCount} output files.`);
    });

} catch (error) {
    console.error('❌ Error setting up file stream:', error.message);
    process.exit(1);
}
