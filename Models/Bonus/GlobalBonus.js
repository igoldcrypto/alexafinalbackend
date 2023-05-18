const mongoose = require('mongoose')


const GlobalBonus = mongoose.Schema(
  {
    BonusOwner: {
      type: 'string',
      require: true
    },
    Percantage: {
      type: 'string',
      require: true
    }
  },
  {
    timestamps: true
  }
)
module.exports = mongoose.model("GlobalBonus", GlobalBonus);

// export default mongoose.models.GlobalBonus || mongoose.model('GlobalBonus', GlobalBonus)
