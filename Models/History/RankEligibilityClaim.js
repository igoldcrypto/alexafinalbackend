const mongoose = require('mongoose')


const RankEligibilityClaim = mongoose.Schema({

    RankEligibilityClaimOwnerId: {
        require: true,
        type: 'String'
    },
    RankEligibilityClaimOwnerUserName: {
        require: true,
        type: 'String'
    },
    RankEligibilityClaimOwnerEmail: {
        require: true,
        type: 'String'
    },
    PackageOwnName: {
        require: true,
        type: 'String'
    },
    PackageOwnPrice: {
        require: true,
        type: 'String'
    },
    ClaimedReward: {
        require: true,
        type: 'String'
    },
    TotBusiness: {
        require: true,
        type: 'String'
    }
},
{
  timestamps: true
})
module.exports = mongoose.model("RankEligibilityClaim", RankEligibilityClaim);

// export default mongoose.models.RankEligibilityClaim || mongoose.model('RankEligibilityClaim', RankEligibilityClaim)
