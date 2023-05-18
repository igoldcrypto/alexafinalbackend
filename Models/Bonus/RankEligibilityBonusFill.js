import mongoose from "mongoose";


const RankEligibilityBonusFill = mongoose.Schema({

    UpperLineUserId:{
        type:"string",
        require:true
    },
    DownLineUserId:{
        type:"string",
        require:true
    },
    BusinessAmount:{
        type:"string",
        require:true
    },
    BusinessMonth:{
        type:"string",
        require:true
    },    
    BusinessYear:{
        type:"string",
        require:true
    }    
    
})
export default mongoose.models.RankEligibilityBonusFill || mongoose.model('RankEligibilityBonusFill', RankEligibilityBonusFill)
