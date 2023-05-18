import mongoose from "mongoose";


const RankBonusHistory = mongoose.Schema({
    UpperLineUserId:{
        type:"string",
        require:true
    },
    UpperLineUserSponser:{
        type:"string",
        require:true
    },
    UpperLineUserEmail:{
        type:"string",
        require:true
    },
    DownLineUserId:{
        type:"string",
        require:true
    },
    DownLineUserSponser:{
        type:"string",
        require:true
    },
    DownLineUserEmail:{
        type:"string",
        require:true
    },
    BusinessAmount:{
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
    }
},
{
    timestamps: true
})
export default mongoose.models.RankBonusHistory || mongoose.model('RankBonusHistory', RankBonusHistory)
