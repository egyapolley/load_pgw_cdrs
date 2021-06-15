const fs = require("fs")
const path = require("path")
const inputDir = path.join(__dirname, "input_dir");
const outputDir = path.join(__dirname, "processed_dir")

const files =fs.readdirSync(inputDir)
if (files.length >0){
    for (const file of files) {
        let inputFilePath = path.join(inputDir, file);
        let outputFilePath = path.join(outputDir, file);
        try {
            console.log(inputFilePath)
            const data = fs.readFileSync(inputFilePath, {encoding: "utf-8"})
            const parsedData = JSON.parse(data)
            parsedData.shift()
        } catch (e) {

        }


    }
}




