const mongoose = require('mongoose')

const ShortRecord = mongoose.Schema({

    RecordOwner:{
        type:"String",
        require:true
    },
    DailyStakig:{
        type:"Number",
        default:0
    },
    PowerStaing:{
        type:"Number",
        default:0
    },
    DirectReward:{
        type:"Number",
        default:0
    },
    MatcingBonus:{
        type:"Number",
        default:0
    },
    RankEligibility:{
        type:"Number",
        default:0
    },
    GobalPoolBonus:{
        type:"Number",
        default:0
    },
    RebuyBonus:{
        type:"Number",
        default:0
    },
    DirectTeam:{
        type:"Number",
        default:0
    },
    isEligibalForMatching:{
        type:"Bool",
        default:true
    },
},
{
  timestamps: true
})
module.exports = mongoose.model("MyShortRecordsds", ShortRecord);

// export default mongoose.models.ShortRecord || mongoose.model('ShortRecord', ShortRecord)
