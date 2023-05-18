const mongoose = require('mongoose')

const GlobalBonusHistory = mongoose.Schema(
  {
    Owner: {
      require: true,
      type: 'String'
    },
    Coins: {
      require: true,
      type: 'String'
    },
    Percantage: {
      require: true,
      type: 'String'
    },
    CompanyBusiness: {
      require: true,
      type: 'String'
    },
  },
  {
    timestamps: true
  }
)
module.exports = mongoose.model("GlobalBonusHistoryn", GlobalBonusHistory);

// export default mongoose.models.GlobalBonusHistory || mongoose.model('GlobalBonusHistory', GlobalBonusHistory)
