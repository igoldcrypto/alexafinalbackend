const mongoose = require('mongoose')


const LapWallet = mongoose.Schema({
    BonusOwner: {
        type: 'String',
        require: true
    },
    LapAmount: {
        type: 'String',
        require: true
    }
},
    {
        timestamps: true
    })
// export default mongoose.models.LapWallet || mongoose.model('LapWallet', LapWallet)
// export default mongoose.models.LapWallet || mongoose.model('LapWallet', LapWallet)

module.exports = mongoose.model("LapWallet", LapWallet);


