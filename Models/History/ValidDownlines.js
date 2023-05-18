const mongoose = require('mongoose')

const ValidDownlines = new mongoose.Schema({
    UpperLineUserId:{
        type:"string",
        require:true
    },
    DownLineUserId:{
        type:"string",
        require:true
    },
    UpperLinePackagePrice:{
        type:"string",
        require:true
    },
    PurchasedPackageName:{
        type:"string",
        require:true
    },
    PurchasedPackagePrice:{
        type:"string",
        require:true
    },
    OldOddCount:{
        type:"Number",
        require:false,
        default:0
    },
},
{
    timestamps: true
})

module.exports = mongoose.model("validdownlines", ValidDownlines);
// export default mongoose.models.ValidDownlines || mongoose.model('ValidDownlines', ValidDownlines)
