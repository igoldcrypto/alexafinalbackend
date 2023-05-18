const PackageHistory = require("../Models/History/PackageHistory")
const DailyBonus = require("../Models/History/DailyBonus")
const LykaFastBonus = require("../Models/Bonus/LykaFastBonus")
const User = require("../Models/User")
const RankEligibilityClaim = require("../Models/History/RankEligibilityClaim")
const GlobalBonus = require("../Models/Bonus/GlobalBonus")
const GlobalBonusHistory = require("../Models/History/GlobalBonusHistory")
const LykaFastBonusHis = require("../Models/History/LykaFastBonusHis")
const RenewalPurchasePackage = require("../Models/Renewal/RenewalPurchasePackage")
const RebuyBonus = require("../Models/Bonus/RebuyBonus")
const ShortRecord = require("../Models/ShortRecord")
const LapWallet = require("../Models/LapWallet")



// GlobalBonus
// GlobalBonusHistory

exports.GlobalBonusMonthly = async (req, res) => {
  let shortRecordArr = [];
  let updateShortRecordArr = [];
  let globalBonusArr = [];
  let globalBonusHistoryArr = [];
  let UpdateUserDetailArr = [];
  const oneMonthAgo = new Date();
  oneMonthAgo.setMonth(oneMonthAgo.getMonth() - 1);
  const users = await User.aggregate([{
      $addFields: {
        userIdz: {
          $convert: {
            input: "$_id",
            to: "string",
            onError: null,
            onNull: null
          }
        }
      }
    },
    {
      $lookup: {
        from: 'rankeligibilityclaims',
        localField: 'userIdz',
        foreignField: 'RankEligibilityClaimOwnerId',
        as: 'RankEligibilityClaim'
      }
    },
    {
      $addFields: {
        RankEligibilityClaim: {
          $slice: ['$RankEligibilityClaim', -1]
        }
      }
    },
    {
      $unwind: {
        path: "$RankEligibilityClaim",
        preserveNullAndEmptyArrays: false,
      },
    },
    {
      $lookup: {
        from: 'myshortrecordsds',
        localField: 'userIdz',
        foreignField: 'RecordOwner',
        as: 'shortRecordDetail'
      }
    },
    {
      $unwind: {
        path: "$shortRecordDetail",
        preserveNullAndEmptyArrays: true,
      },
    },
  ]);

  const RankBonusHistoryData = await PackageHistory.aggregate([{
      $match: {
        "createdAt": {
          $gte: oneMonthAgo
        }
      }
    },
    {
      $group: {
        _id: null,
        TotalBusiness: {
          $sum: {
            $convert: {
              input: "$PackagePrice",
              to: "double",
              onError: 0,
              onNull: 0
            }
          }
        }
      }
    },
  ]);

  const totalBusiness = RankBonusHistoryData.length ? RankBonusHistoryData[0].TotalBusiness : 0;


  for (let index = 0; index < users.length; index++) {
    console.log("users.length ========== ", users[index])
    const PRICE_LOOKUP_TABLE = {
      250: {
        percentage: 2,
        star: "1 Star Eligible"
      },
      500: {
        percentage: 1,
        star: "2 Star Eligible"
      },
      1000: {
        percentage: 0.5,
        star: "3 Star Eligible"
      },
      2500: {
        percentage: 0.3,
        star: "4 Star Eligible"
      },
      5000: {
        percentage: 0.2,
        star: "5 Star Eligible"
      },
      10000: {
        percentage: 0.1,
        star: "6 Star Eligible"
      },
      25000: {
        percentage: 0.1,
        star: "7 Star Eligible"
      },
      50000: {
        percentage: 0.1,
        star: "8 Star Eligible"
      },
    };

    const mainUserPackagePrice = Number(users[index].RankEligibilityClaim.ClaimedReward)

    const rankEligibleForThatPackage = await RankEligibilityClaim.find({
      ClaimedReward: mainUserPackagePrice
    })

    const memberEligible = rankEligibleForThatPackage.length

    const {
      percentage,
      star
    } = PRICE_LOOKUP_TABLE[mainUserPackagePrice] || {
      percentage: 0,
      star: ""
    };

    let percantage = percentage;

    const My_Wallet = Number(users[index].MainWallet);
    var capping = Number(users[index].PurchasedPackagePrice) * 300/100;
    var est1 = Number(totalBusiness) * percantage / 100
    var givre = Number(est1) / Number(memberEligible)

    if(My_Wallet + givre > capping){
      givre = givre - (My_Wallet + givre - capping)
    }

    console.log("checks ----- ======== ", My_Wallet, givre, capping)

    // const esDate = new Date(start)

    globalBonusArr.push({
      BonusOwner: users[index]._id,
      Percantage: percantage
    });

    globalBonusHistoryArr.push({
      Owner: users[index]._id,
      Coins: givre,
      Percantage: percantage,
      CompanyBusiness: totalBusiness
    })


    if (users[index].shortRecordDetail) {
      let sum = (parseFloat(users[index].shortRecordDetail.GobalPoolBonus) + parseFloat(givre)).toFixed(2)

      updateShortRecordArr.push({
        "updateOne": {
          "filter": {
            "_id": users[index].shortRecordDetail._id
          },
          "update": {
            $set: {
              "GobalPoolBonus": sum
            }
          }
        }
      })
    } else {
      shortRecordArr.push({
        RecordOwner: users[index]._id,
        GobalPoolBonus: Number(givre).toFixed(2)
      })
    }

    if(givre>0) {
      console.log("index ------- ====== ", index)
      
      UpdateUserDetailArr?.push({
        "updateOne": {
          "filter": { "_id": users[index]._id },
          "update": { $set: { "MainWallet": My_Wallet + givre } }
        }
      })
    }
  }
  console.log("myshortrecordsds UpdateUserDetailArr ========== ", JSON.stringify(UpdateUserDetailArr))


  globalBonusArr.length > 0 && await GlobalBonus.insertMany(globalBonusArr);
  globalBonusHistoryArr.length > 0 && await GlobalBonusHistory.insertMany(globalBonusHistoryArr);
  shortRecordArr.length > 0 && await ShortRecord.insertMany(shortRecordArr);
  updateShortRecordArr.length > 0 && await ShortRecord.bulkWrite(updateShortRecordArr);
  UpdateUserDetailArr.length > 0 && await User.bulkWrite(UpdateUserDetailArr);

  return res.json(globalBonusHistoryArr)
}