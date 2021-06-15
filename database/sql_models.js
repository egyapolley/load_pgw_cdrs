const Sequelize = require("sequelize");

const sequelize = require("./sql_database");


const CDR = sequelize.define("cdrs", {
    id: {
        type:Sequelize.INTEGER,
        primaryKey:true,
        allowNull:false,
        autoIncrement:true
    },
    imsi: {
        type:Sequelize.STRING,
        allowNull: true,
    },
    sGWAddress: {
        type:Sequelize.STRING,
        allowNull: true,

    },
    chargingID: {
        type:Sequelize.STRING,
        allowNull: true,
    },
    servingNodeAddress: {
        type:Sequelize.STRING,
        allowNull: true,
    },
    recordOpeningTime: {
        type:Sequelize.STRING,
        allowNull: true,
    },
    duration: {
        type:Sequelize.STRING,
        allowNull: true,
    },
    causeForRecClosing: {
        type:Sequelize.STRING,
        allowNull: true,
    },
    recordSequenceNumber: {
        type:Sequelize.STRING,
        allowNull: true,
    },
    msisdn: {
        type:Sequelize.STRING,
        allowNull: true,

    },
    chargingCharacteristics: {
        type:Sequelize.STRING,
        allowNull: true,
    },
    userLocationInformation: {
        type:Sequelize.STRING,
        allowNull: true,
    },
    startTime: {
        type:Sequelize.STRING,
        allowNull: true,
    },
    stopTime: {
        type:Sequelize.STRING,
        allowNull: true,

    },
    totalVolume: {
        type:Sequelize.STRING,
        allowNull: true,

    },
    servedPDPPDNAddress:{
        type:Sequelize.STRING,
        allowNull: true,
    },
    pDNConnectionChargingID:{
        type:Sequelize.STRING,
        allowNull: true,
    }

});


module.exports = {CDR}

