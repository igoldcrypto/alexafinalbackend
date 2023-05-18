const mongoose = require('mongoose')



const RebuyBonus = mongoose.Schema({

    BonusOwner:{
        default: 'null',
        type: 'String'
    },
    ReferSentFromId:{
        default: 'null',
        type: 'String'
    },
    ReferSentFromUserId:{
        default: 'null',
        type: 'String'
    },
    ReferGetFromId:{
        default: 'null',
        type: 'String'
    },
    ReferGetFromUserId:{
        default: 'null',
        type: 'String'
    },
    PackName:{
        default: 'null',
        type: 'String'
    },
    EarnedRewardCoins:{
        default: 'null',
        type: 'String'
    }
},
{
  timestamps: true
})
module.exports = mongoose.model("RebuyBonus", RebuyBonus);

// export default mongoose.models.RebuyBonus || mongoose.model('RebuyBonus', RebuyBonus)
