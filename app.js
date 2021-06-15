const fs = require("fs")
const path = require("path")
const JSONStream = require('JSONStream')
const moment = require("moment")

const sequelize = require("./database/sql_database")
const CDR = require("./database/sql_models").CDR

//const inputDir = path.join(__dirname, "input_dir");
const inputDir ="/home/inadmin/cli-scripts/load_pgw_cdrs/temp_input"
const outputDir = path.join(__dirname, "processed_dir")

const files = fs.readdirSync(inputDir)
if (files.length > 0) {
    let counter=0
    for (const file of files) {
        const dataArray =[]
        try {
            let inputFilePath = path.join(inputDir, file);
            let outputFilePath = path.join(outputDir, file);

            const fileStream = fs.createReadStream(inputFilePath, {encoding: 'utf-8'})
            const parser = JSONStream.parse("*.sGWRecord")
            fileStream.pipe(parser)


            parser.on("data", function (obj) {
                const sGWRecord =obj
                if (sGWRecord[1].servedIMSI.startsWith("6200802")) {
                    let imsi, sGWAddress, chargingID, servingNodeAddress, recordOpeningTime,
                        duration, causeForRecClosing, recordSequenceNumber, servedPDPPDNAddress,
                        msisdn, chargingCharacteristics, userLocationInformation,
                        startTime, stopTime, pDNConnectionChargingID, totalVolume = 0

                    for (const sGWRecordElement of sGWRecord) {
                        if (sGWRecordElement.servedIMSI) imsi = sGWRecordElement.servedIMSI
                        if (sGWRecordElement['s-GWAddress']) sGWAddress = sGWRecordElement['s-GWAddress'][0].IPAddress
                        if (sGWRecordElement.chargingID) chargingID = sGWRecordElement.chargingID
                        if (sGWRecordElement.servingNodeAddress) servingNodeAddress = (sGWRecordElement.servingNodeAddress)[0].IPAddress
                        if (sGWRecordElement.servedPDPPDNAddress) servedPDPPDNAddress = (sGWRecordElement.servedPDPPDNAddress)[0].PDPAddress[0].IPAddress
                        if (sGWRecordElement.listOfTrafficVolumes) {
                            const listOfTrafficData = sGWRecordElement.listOfTrafficVolumes
                            for (const listOfTrafficElement of listOfTrafficData) {
                                if (listOfTrafficElement.ChangeOfCharCondition) {
                                    const changeOfCharCondition = listOfTrafficElement.ChangeOfCharCondition

                                    for (const changeOfCharConditionElement of changeOfCharCondition) {
                                        if (changeOfCharConditionElement.datavolumeFBCUplink !== undefined) {
                                            totalVolume += changeOfCharConditionElement.datavolumeFBCUplink
                                        }

                                        if (changeOfCharConditionElement.datavolumeFBCDownlink !== undefined) {
                                            totalVolume += changeOfCharConditionElement.datavolumeFBCDownlink
                                        }

                                    }


                                }


                            }


                        }
                        if (sGWRecordElement.recordOpeningTime) recordOpeningTime = sGWRecordElement.recordOpeningTime
                        if (sGWRecordElement.duration !== undefined) duration = sGWRecordElement.duration
                        if (sGWRecordElement.causeForRecClosing) causeForRecClosing = sGWRecordElement.causeForRecClosing
                        if (sGWRecordElement.recordSequenceNumber) recordSequenceNumber = sGWRecordElement.recordSequenceNumber
                        if (sGWRecordElement.servedMSISDN) msisdn = sGWRecordElement.servedMSISDN
                        if (sGWRecordElement.chargingCharacteristics) chargingCharacteristics = sGWRecordElement.chargingCharacteristics
                        if (sGWRecordElement.userLocationInformation) userLocationInformation = sGWRecordElement.userLocationInformation
                        if (sGWRecordElement.startTime) startTime = sGWRecordElement.startTime
                        if (sGWRecordElement.stopTime) stopTime = sGWRecordElement.stopTime
                        if (sGWRecordElement.pDNConnectionChargingID) pDNConnectionChargingID = sGWRecordElement.pDNConnectionChargingID

                        startTime = startTime?moment(startTime, "YYYY-MM-DD HH:mm:ss Z").format("YYYYMMDDHHmmss"):null
                        stopTime = stopTime?moment(stopTime, "YYYY-MM-DD HH:mm:ss Z").format("YYYYMMDDHHmmss"):null

                    }




                    /*         await CDR.create({imsi, sGWAddress, chargingID, servingNodeAddress, recordOpeningTime,
                                 duration, causeForRecClosing, recordSequenceNumber,
                                 msisdn, chargingCharacteristics, userLocationInformation,
                                 startTime, stopTime,totalVolume,pDNConnectionChargingID,servedPDPPDNAddress
                             })*/
                    dataArray.push({
                        imsi, sGWAddress, chargingID, servingNodeAddress, recordOpeningTime,
                        duration, causeForRecClosing, recordSequenceNumber,
                        msisdn, chargingCharacteristics, userLocationInformation,
                        startTime, stopTime,totalVolume,pDNConnectionChargingID,servedPDPPDNAddress
                    })
                    counter++
                    console.log("=============================================================")
                    console.log(`${new Date() } Total Records Inserted: ${counter}`)
                    console.log("===========================================================")

                }


            })
            fileStream.on("end", async function () {
                await CDR.bulkCreate(dataArray)
                console.log(fileStream.path +" processed")

            })

        } catch (e) {
            console.log(e)
        }




    }




}


