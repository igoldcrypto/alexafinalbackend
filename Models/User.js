const mongoose = require('mongoose')



const User = new mongoose.Schema(
  {
    UserName: {
      default: 'null',
      type: 'String'
    },
    SponserCode: {
      default: 'null',
      type: 'String'
    },
    FullName: {
      default: 'null',
      type: 'String'
    },
    Position: {
      default: 'null',
      type: 'String'
    },
    Country: {
      default: 'null',
      type: 'String'
    },
    ContactNumber: {
      default: 0,
      type: 'Number'
    },
    EmailId: {
      default: 'null',
      type: 'String'
    },
    EmailVerified: {
      default: 'Yes',
      type: 'String'
    },
    UserActive: {
      default: true,
      type: 'Bool'
    },
    MainWallet: {
      default: 0,
      type: 'Number'
    },
    UpperlineUser: {
      default: 'null',
      type: 'String'
    },
    Passsword: {
      default: 'null',
      type: 'String'
    },
    LeftTeamId: {
      default: 'null',
      type: 'String'
    },
    RightTeamId: {
      default: 'null',
      type: 'String'
    },
    LeftTeamName: {
      default: 'null',
      type: 'String'
    },
    RightTeamName: {
      default: 'null',
      type: 'String'
    },
    MatchingBonusWallet: {
      default: '0',
      type: 'String',
      required:true
    },
    PurchasedPackageName: {
      type: 'String',
      required:false
    },
    PurchasedPackagePrice: {
      default: 0,
      type: 'Number',
      required:false
    },
    PurchasedPackageDate: {
      type: 'String',
      required:false
    },
    UserEarnPercantage:{
      type: 'String',
      default:"0%"
    },
    PreviousPercentage:{
      default: 0,
      type: 'Number',
      required:false
    }

  },
  {
    timestamps: true
  }
)
module.exports = mongoose.model("myuserp", User);
// export default mongoose.models.myuserp || mongoose.model('myuserp', User)
