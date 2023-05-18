import mongoose from 'mongoose'

const ReferalHistory = mongoose.Schema(
  {
    ReferralFrom: {
      require: true,
      type: 'String'
    },
    ReferralTo: {
      require: true,
      type: 'String'
    },
    ReferralCoins: {
      require: true,
      type: 'String'
    },
    ReferralPercantage: {
      require: true,
      type: 'String'
    },
    PackageName: {
      require: true,
      type: 'String'
    },
    Type: {
      type: 'String',
      default:"referal"
    },
  },
  {
    timestamps: true
  }
)
export default mongoose.models.ReferalHistory || mongoose.model('ReferalHistory', ReferalHistory)
