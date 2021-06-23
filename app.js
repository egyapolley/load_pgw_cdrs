const fs = require("fs")
const path = require("path")
const JSONStream = require('JSONStream')
const moment = require("moment")
const es = require("event-stream")

const sequelize = require("./database/sql_database")
const CDR = require("./database/sql_models").CDR

sequelize.sync()
    .then(() => {
        console.log("MySQL successfully connected")
        const inputDir = path.join(__dirname, "input_dir");

        const files = fs.readdirSync(inputDir)
        if (files.length > 0) {
            for (const file of files) {
                try {
                    let inputFilePath = path.join(inputDir, file);

                    const fileStream = fs.createReadStream(inputFilePath, {encoding: 'utf-8'})
                    const parser = JSONStream.parse("*.sGWRecord")
                    fileStream
                        .pipe(parser)
                        .pipe(es.through(function (data) {
                                if (data[1].servedIMSI.startsWith("6200802")) {
                                    this.pause()
                                    processData(data, this, file)

                                }
                                return data;

                            },
                            function end() {
                                this.emit("end")

                            }
                        ))

                } catch (e) {
                    console.log("Error processing file " + file)
                    console.log(e)
                }


            }


        }
    }).catch(error => {
    console.log(error)
})

function processData(data, es, file) {
    const sGWRecord = data
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

            startTime = startTime ? moment(startTime, "YYYY-MM-DD HH:mm:ss Z").format("YYYYMMDDHHmmss") : null
            stopTime = stopTime ? moment(stopTime, "YYYY-MM-DD HH:mm:ss Z").format("YYYYMMDDHHmmss") : null

        }


        CDR.create({
            imsi, sGWAddress, chargingID, servingNodeAddress, recordOpeningTime,
            duration, causeForRecClosing, recordSequenceNumber,
            msisdn, chargingCharacteristics, userLocationInformation,
            startTime, stopTime, totalVolume, pDNConnectionChargingID, servedPDPPDNAddress,
            fileName: file
        }).then(function () {
            es.resume()

        }).catch(error => {
            console.log(error)
            es.resume()
        })


    } else {
        es.resume()
    }


}


