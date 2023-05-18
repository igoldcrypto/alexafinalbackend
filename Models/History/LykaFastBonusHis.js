const mongoose = require('mongoose')


const LykaFastBonusHis = mongoose.Schema(
    {
        BonusOwner: {
          type: 'String',
          require: true
        },
        FormPackage: {
          type: 'String',
          require: true
        },
        PackagePercantage: {
          type: 'String',
          require: true
        },
        Amount: {
          type: 'String',
          require: true
        }
      },
      {
        timestamps: true
      }
)
module.exports = mongoose.model("LykaFastBonusHis", LykaFastBonusHis);

// export default mongoose.models.LykaFastBonusHis || mongoose.model('LykaFastBonusHis', LykaFastBonusHis)
