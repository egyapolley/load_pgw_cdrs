const fs = require("fs")
const path = require("path")

const moment = require("moment")
const sequelize = require("./database/sql_database")
const CDR = require("./database/sql_models").CDR


const inputDir = path.join(__dirname, "input_dir");
const outputDir = path.join(__dirname, "processed_dir")

const stream = fs.createWriteStream("append.txt", {flags: 'a'});

sequelize.sync({logging:false}).then(() => {
    console.log("Mysql DB successfully connected");
    const files =fs.readdirSync(inputDir)
    if (files.length >0){
        let counter=0;
        for (const file of files) {
            let inputFilePath = path.join(inputDir, file);
            let outputFilePath = path.join(outputDir, file);
            try {
                console.log(inputFilePath)
                const data = fs.readFileSync(inputFilePath, {encoding: "utf-8"})
                const parsedData = JSON.parse(data)
                parsedData.shift()
                for (const cdrRecord of parsedData) {
                    if (cdrRecord.sGWRecord) {
                        let sGWRecord = cdrRecord.sGWRecord

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

                            CDR.create({imsi, sGWAddress, chargingID, servingNodeAddress, recordOpeningTime,
                                duration, causeForRecClosing, recordSequenceNumber,
                                msisdn, chargingCharacteristics, userLocationInformation,
                                startTime, stopTime,totalVolume,pDNConnectionChargingID,servedPDPPDNAddress

                            }).then(created =>{
                                counter++
                                console.log("=============================================================")
                                console.log("File processed : "+inputFilePath)
                                console.log(`${new Date() } Total Records Inserted: ${counter}`)
                                console.log("===========================================================")

                            }).catch(error =>console.log(error))

                        }


                    }


                }

            } catch (e) {
                console.log("Error in reading file", inputFilePath)

            }finally {
                fs.renameSync(inputFilePath,outputFilePath)
            }




        }


    }



/*    fs.readdir(inputDir, (dirError, files) => {
        if (dirError) throw dirError;
        if (files.length > 0) {
            let counter = 0;
            for (const file of files) {
                let inputFilePath = path.join(inputDir, file);
                let outputFilePath = path.join(outputDir, file);
                fs.readFile(inputFilePath, {encoding: "utf-8"}, async (fileError, data) => {
                    if (fileError) console.log(fileError)
                    try {
                        const parsedData = JSON.parse(data)
                        parsedData.shift()


                        for (const cdrRecord of parsedData) {
                            if (cdrRecord.sGWRecord) {
                                let sGWRecord = cdrRecord.sGWRecord

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

                                        startTime = moment(startTime, "YYYY-MM-DD HH:mm:ss Z").format("YYYYMMDDHHmmss")
                                        stopTime = moment(stopTime, "YYYY-MM-DD HH:mm:ss Z").format("YYYYMMDDHHmmss")


                                        // await CDR.create({imsi, sGWAddress, chargingID, servingNodeAddress, recordOpeningTime,
                                        //     duration, causeForRecClosing, recordSequenceNumber,
                                        //     msisdn, chargingCharacteristics, userLocationInformation,
                                        //     startTime, stopTime,totalVolume,pDNConnectionChargingID,servedPDPPDNAddress
                                        //
                                        // })


                                    }
                                    counter++
                                    stream.write(`${msisdn},${totalVolume},${startTime},${stopTime},${imsi},${pDNConnectionChargingID},${servedPDPPDNAddress}\n`)


                                }

                            }


                        }


                    } catch (error) {
                        console.log(error)
                    }

                    fs.rename(inputFilePath,outputFilePath, err => {
                        if (err) {
                            console.log(err)
                            console.log(file +" movement failed");
                        }


                    })
                })



            }
            console.log("=============================================================")
            console.log(`${new Date() } Total Records Inserted: ${counter}`)
            console.log("===========================================================")

        }

    })*/
}).catch(error => {
    console.log("Unable to connect to Mysql DB")
    console.log(error)
})
