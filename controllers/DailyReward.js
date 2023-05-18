const PackageHistory = require("../Models/History/PackageHistory")
const DailyBonus = require("../Models/History/DailyBonus")
const LykaFastBonus = require("../Models/Bonus/LykaFastBonus")
const User = require("../Models/User")
const LykaFastBonusHis = require("../Models/History/LykaFastBonusHis")
const LapWallet = require("../Models/LapWallet")
const RenewalPurchasePackage = require("../Models/Renewal/RenewalPurchasePackage")
const RebuyBonus = require("../Models/Bonus/RebuyBonus")
const ShortRecord = require("../Models/ShortRecord")
const PurchasePackageInvoice = require("../Models/Invoice/PurchasePackageInvoice")

exports.homes = async (req, res) => {
  let LapWalletArr = []
  let UpdateLapWalletArr = []
  let RebuyBonusArr = []
  let ShortRecordArr = []
  let UpdateShortRecordArr = []
  let LykaFastBonusHisArr = []
  let UpdateUserDetailArr = []

  let DailyBonusArr = []

  const findPackage = await PackageHistory.aggregate([
    {
      $addFields: {
        userId: { $toObjectId: "$PackageOwner" }
      }
    },
    {
      $lookup: {
        from: 'myuserps',
        localField: 'userId',
        foreignField: "_id",
        as: 'user_detail'
      }
    },
    {
      $unwind: {
        path: "$user_detail",
        preserveNullAndEmptyArrays: true,
      },
    },
    {
      $addFields: {
        upperLineUserId: {
          $ifNull: ["$user_detail.UpperlineUser", ""]
        }
      }
    },
    {
      $addFields: {
        upperLineUserId: {
          $convert: {
            input: "$upperLineUserId",
            to: "objectId",
            onError: null,
            onNull: null
          }
        }
      }
    },
    {
      $lookup: {
        from: 'myuserps',
        localField: 'upperLineUserId',
        foreignField: '_id',
        as: 'UpperlineUserDetail'
      }
    },
    {
      $unwind: {
        path: "$UpperlineUserDetail",
        preserveNullAndEmptyArrays: true,
      },
    },
    {
      $addFields: {
        upperLineUserId: {
          $ifNull: ["$user_detail.UpperlineUser", ""]
        }
      }
    },
    {
      $lookup: {
        from: 'packagehistries',
        localField: 'upperLineUserId',
        foreignField: 'PackageOwner',
        as: 'UpperlineUserPackageDetails'
      }
    },
    {
      $unwind: {
        path: "$UpperlineUserPackageDetails",
        preserveNullAndEmptyArrays: true,
      },
    },
    {
      $addFields: {
        upperLineUserId: {
          $ifNull: ["$user_detail.UpperlineUser", ""]
        }
      }
    },
    {
      $lookup: {
        from: 'myshortrecordsds',
        localField: 'upperLineUserId',
        foreignField: 'RecordOwner',
        as: 'UpperlineUserShortDetails'
      }
    },
    {
      $unwind: {
        path: "$UpperlineUserShortDetails",
        preserveNullAndEmptyArrays: true,
      },
    },
    {
      $addFields: {
        upperLineUserId: {
          $ifNull: ["$user_detail.UpperlineUser", ""]
        }
      }
    },
    {
      $lookup: {
        from: 'lapwallets',
        localField: 'upperLineUserId',
        foreignField: 'BonusOwner',
        as: 'UpperlineUserLapWalletDetails'
      }
    },
    {
      $unwind: {
        path: "$UpperlineUserLapWalletDetails",
        preserveNullAndEmptyArrays: true,
      },
    },
    {
      $lookup: {
        from: 'renewalpurchasepackages',
        let: { ownerId: '$PackageOwner' },
        pipeline: [
          {
            $match: {
              $expr: {
                $and: [
                  { $eq: ['$PackageOwner', '$$ownerId'] },
                  // { $eq: ['$RenewalStatus', 'SUCCESS'] }
                ]
              }
            }
          },
          {
            $sort: { createdAt: -1 }
          },
          {
            $limit: 1
          }
        ],
        as: 'Renewal_Detail'
      }
    },
    {
      $addFields: {
        Renewal_Detail: { $arrayElemAt: ['$Renewal_Detail', 0] }
      }
    },
    {
      $unwind: {
        path: "$Renewal_Detail",
        preserveNullAndEmptyArrays: true,
      },
    },
    {
      $addFields: {
        upperLineUserId: {
          $ifNull: ["$Renewal_Detail", ""]
        }
      }
    },
    {
      $lookup: {
        from: 'lykafastbonus',
        localField: 'PackageOwner',
        foreignField: 'FastBonusCandidate',
        as: 'Lyka_Detail'
      }
    },
    {
      $unwind: {
        path: "$Lyka_Detail",
        preserveNullAndEmptyArrays: true,
      },
    },
    {
      $addFields: {
        upperLineUserId: {
          $ifNull: ["$Lyka_Detail", ""]
        }
      }
    },
    {
      $lookup: {
        from: 'myshortrecordsds',
        localField: 'PackageOwner',
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
    {
      $addFields: {
        upperLineUserId: {
          $ifNull: ["$shortRecordDetail", ""]
        }
      }
    },
    {
      $lookup: {
        from: 'lapwallets',
        localField: 'PackageOwner',
        foreignField: 'BonusOwner',
        as: 'lapWalletDetail'
      }
    },
    {
      $unwind: {
        path: "$lapWalletDetail",
        preserveNullAndEmptyArrays: true,
      },
    },
    {
      $addFields: {
        upperLineUserId: {
          $ifNull: ["$lapWalletDetail", ""]
        }
      }
    },
  ])
  console.log((findPackage.map(item => item.PackageOwner)).length, "dihei")

  if (findPackage.length == 0) {
    return res.json("No user found")
  }

  for (let i = 0; i < findPackage.length; i++) {
    // console.log("UpdateShortRecordArr ================ ", findPackage[i].PackageOwner)
    const investedAmount = findPackage[i].PackagePrice
    var per = 0.3
    const myOldWallet = findPackage[i].user_detail

    if (myOldWallet.UpperlineUser !== "null") {
      const upperlineUserDatas = findPackage[i].UpperlineUserDetail;
      // console.log("upperlineUserDatas._id ========== ---------- ", upperlineUserDatas._id)

      const FindPackages = findPackage[i].UpperlineUserPackageDetails
      console.log("FindPackages ========== ---------- ", upperlineUserDatas)

      const Max_Caps = Number(FindPackages.PackagePrice) * 300 / 100
      var finalCals = (Number(investedAmount) * per) / 100
      const Got_Rewards = Number(finalCals) * 3 / 100
      const My_Wallets = Number(myOldWallet.MainWallet)

      if (Got_Rewards + My_Wallets >= Max_Caps) {
        continue;
      }
    } else {
      const investedAmount = findPackage[i].PackagePrice

      const Max_Caps = Number(investedAmount) * 300 / 100
      var finalCals = (Number(investedAmount) * per) / 100
      const Got_Rewards = Number(finalCals) * 3 / 100
      const My_Wallets = findPackage[i].user_detail.MainWallet

      if (Got_Rewards + My_Wallets >= Max_Caps) {
        continue;
      }
    }

    // console.log("Got_Rewards + My_Wallets >= Max_Caps ============== ", Got_Rewards, findPackage[i].user_detail.MainWallet, Max_Caps)

    var findFastBonus = findPackage[i].Lyka_Detail
    const findRenewalBonus = findPackage[i].Renewal_Detail

    var FindMainUserReferals = []
    if (findFastBonus?.length !== 0 && typeof findFastBonus != "undefined") {
      var totLenght = findFastBonus.ReferLength
      const findMainUser = findFastBonus.FastBonusCandidate
      const findUserPackage = await PackageHistory.findOne({
        PackageOwner: findMainUser
      })

      const MainUserPackagePrice = findUserPackage.PackagePrice

      FindMainUserReferals = await User.find({
        UpperlineUser: findMainUser,
        PurchasedPackagePrice: {
          $gte: Number(MainUserPackagePrice)
        },
        createdAt: {
          $gte: findPackage[i].createdAt
        }
      })
      console.log("FindMainUserReferals ============= ", FindMainUserReferals.length)

      // const TotalBuy = await PurchasePackageInvoice.find({
      //   PackageOwner: findPackage[i].PackageOwner,
      //   Type: "Repurchased"
      // }).count()

      let userLength = findPackage[i].Type == "Basic" ? FindMainUserReferals.length : (FindMainUserReferals.length>0 ? FindMainUserReferals.length - 1 : 0);
      // if(findPackage[i].Type!="Basic"){
      //   if(userLength>1){
      //     userLength = userLength - 1
      //   }
      // }

      // console.log("userLength ============= ", userLength, findMainUser)

      if (userLength == 2 || userLength == 3) {
        per = 1

      }else if(userLength == 4 || userLength == 5){
        per = 2

      }else if(userLength == 6 || userLength == 7){
        per = 3

      }else if(userLength == 8 || userLength == 9){
        per = 4

      }else if(userLength >= 10){
        per = 5

      }
      console.log("userLength per ============= ", per)

      if(myOldWallet?.PreviousPercentage > 0) {
        if(per<1){
          per = Number(myOldWallet?.PreviousPercentage);
        } else {
          per = per + Number(myOldWallet?.PreviousPercentage);
        }
      }
      console.log("userLength 11111111 ============= ", userLength, FindMainUserReferals.length, findPackage[i].PackageOwner, per, myOldWallet?.PreviousPercentage)


      if (findRenewalBonus==null) {      

      }else if(findRenewalBonus!==null&&findRenewalBonus.DirectReferalDone == "false"){

        per = 0.3
      }else if(findRenewalBonus.DirectReferalDone == "false"){

        per = 0.3
      }else{

      }
    }

    // console.log("per ============= ", per)

    var finalCal = (Number(investedAmount) * per) / 100
    var refDef = finalCal * 3 / 100
    var myWallete = Number(myOldWallet.MainWallet)
    var finalWallete = Number(myWallete) + Number(finalCal)

    if (myOldWallet.UpperlineUser !== "null") {
      const upperlineUserDatas = findPackage[i].UpperlineUserDetail;
      const FindPackagesforThis = findPackage[i];

      if (FindPackagesforThis.Type == "Repurchased") {
        if (Number(myOldWallet.PurchasedPackagePrice) > 0) {
          const FindPackage = findPackage[i].UpperlineUserPackageDetails

          const Max_Cap = Number(FindPackage.PackagePrice) * 300 / 100
          const Got_Reward = Number(finalCal) * 3 / 100
          const My_Wallet = Number(upperlineUserDatas.MainWallet)
          var Add_Money_In_Wallet = 0;

          if (Got_Reward + My_Wallet >= Max_Cap) {
            Add_Money_In_Wallet = Max_Cap - My_Wallet
            if(Add_Money_In_Wallet>0){
              const Lap_Income = Got_Reward > Add_Money_In_Wallet ? Add_Money_In_Wallet : Got_Reward
              const fetch_Last_Lap_Wallet = findPackage[i].UpperlineUserLapWalletDetails

              let check = LapWalletArr.length == 0 ? -1 : LapWalletArr.findIndex((value) => value.BonusOwner === fetch_Last_Lap_Wallet?._id?.toString())
              let checkUpdate = UpdateLapWalletArr.length == 0 ? -1 : UpdateLapWalletArr.findIndex((value) => value?.updateOne.filter.BonusOwner === upperlineUserDatas._id.toString())

              if (check == -1 && !fetch_Last_Lap_Wallet) {
                LapWalletArr.push({
                  BonusOwner: upperlineUserDatas._id.toString(),
                  LapAmount: Lap_Income
                })
              } else if (checkUpdate == -1 && fetch_Last_Lap_Wallet) {
                UpdateLapWalletArr?.push({
                  "updateOne": {
                    "filter": { "BonusOwner": upperlineUserDatas._id.toString() },
                    "update": { $set: { "LapAmount": Lap_Income + fetch_Last_Lap_Wallet.LapAmount } }
                  }
                })
              } else {
                if (fetch_Last_Lap_Wallet) {
                  UpdateLapWalletArr[checkUpdate].updateOne.update.$set.LapAmount += Lap_Income
                } else {
                  let sum = Number(Lap_Income) + Number(LapWalletArr[check].LapAmount)
                  LapWalletArr[check].LapAmount = sum
                }
              }
              // console.log("Add_Money_In_Wallet -------- ======== ", Add_Money_In_Wallet)

              RebuyBonusArr.push({
                BonusOwner: upperlineUserDatas._id,
                ReferSentFromId: myOldWallet._id,
                ReferSentFromUserId: myOldWallet.UserName,
                ReferGetFromId: upperlineUserDatas._id,
                ReferGetFromUserId: upperlineUserDatas.UserName,
                PackName: myOldWallet.PurchasedPackageName,
                EarnedRewardCoins: Number(Add_Money_In_Wallet).toFixed(2)
              })
            }
          } else {
            Add_Money_In_Wallet = Got_Reward
            // console.log("Got_Reward =========== ------ ", Got_Reward)
            RebuyBonusArr.push({
              BonusOwner: upperlineUserDatas._id,
              ReferSentFromId: myOldWallet._id,
              ReferSentFromUserId: myOldWallet.UserName,
              ReferGetFromId: upperlineUserDatas._id,
              ReferGetFromUserId: upperlineUserDatas.UserName,
              PackName: myOldWallet.PurchasedPackageName,
              EarnedRewardCoins: Number(Got_Reward).toFixed(2)
            })
          }

          if(Add_Money_In_Wallet>0){
            let checkUpdate = UpdateUserDetailArr.length == 0 ? -1 : UpdateUserDetailArr.findIndex((value) => value.updateOne.filter._id.toString() == upperlineUserDatas._id)

            if(checkUpdate == -1){
              UpdateUserDetailArr?.push({
                "updateOne": {
                  "filter": { "_id": upperlineUserDatas._id },
                  "update": { $set: { "MainWallet": My_Wallet + Add_Money_In_Wallet } }
                }
              })
            } else {
              UpdateUserDetailArr[checkUpdate].updateOne.update.$set.MainWallet += Add_Money_In_Wallet
            }
          }

          if(Add_Money_In_Wallet>0){
            const findShortRecord = findPackage[i].UpperlineUserShortDetails;
            let check = ShortRecordArr.length == 0 ? -1 : ShortRecordArr.findIndex((value) => value.RecordOwner === findPackage[i].UpperlineUserDetail._id.toString())
            let checkUpdate = UpdateShortRecordArr.length == 0 ? -1 : UpdateShortRecordArr.findIndex((value) => value?.updateOne.filter.RecordOwner === findPackage[i].UpperlineUserDetail._id.toString())

            if (check == -1 && !findShortRecord) {
              // console.log("Got_Reward 2222222 =========== ------ ", Got_Reward)
              ShortRecordArr.push({
                RecordOwner: upperlineUserDatas._id.toString(),
                RebuyBonus: Number(Number(Add_Money_In_Wallet).toFixed(2))
              })
            } else if (checkUpdate == -1 && findShortRecord) {
              // console.log("findShortRecord.RebuyBonus ========  =========== ------ ", findShortRecord.RebuyBonus)
              let sum = Number((parseFloat(findShortRecord.RebuyBonus) + parseFloat(Add_Money_In_Wallet)).toFixed(2))
              console.log("sum 1111 ========  =========== ------ ", sum, findPackage[i].PackageOwner)
              UpdateShortRecordArr?.push({
                "updateOne": {
                  "filter": { "RecordOwner": upperlineUserDatas._id.toString() },
                  "update": { $set: { "RebuyBonus": sum } }
                }
              })
            } else {
              // console.log("Number(findShortRecord.RebuyBonus) + Number(Got_Reward) ========  =========== ------ ", Number(findShortRecord.RebuyBonus) + Number(Got_Reward))
              if (findShortRecord) {
                let total = 0;
                if(UpdateShortRecordArr[checkUpdate].updateOne.update.$set["RebuyBonus"] === undefined) {
                  total = Number(findShortRecord.RebuyBonus) + Number(Add_Money_In_Wallet);
                  UpdateShortRecordArr[checkUpdate].updateOne.update.$set.RebuyBonus = 0;
                } else {
                  total = Number(Add_Money_In_Wallet);
                }
                UpdateShortRecordArr[checkUpdate].updateOne.update.$set.RebuyBonus += total
              } else {
                let sum = Number((parseFloat(ShortRecordArr[check].RebuyBonus) + parseFloat(Add_Money_In_Wallet)).toFixed(2))
                ShortRecordArr[check].RebuyBonus = sum
              }
            }
          }
        }
      }
    }

    if (findRenewalBonus !== null && findRenewalBonus?.DirectReferalDone == "true") {
      if (per>=1) {
        const FindPackage = findPackage[i];

        const Max_Cap = Number(FindPackage.PackagePrice) * 300 / 100
        const Got_Reward = Number(finalCal)
        const My_Wallet = Number(myOldWallet.MainWallet)
        var Add_Money_In_Wallet = 0;

        if (Got_Reward + My_Wallet >= Max_Cap) {
          Add_Money_In_Wallet = Max_Cap - My_Wallet
          if (Add_Money_In_Wallet > 0) {
            const Lap_Income = Got_Reward > Add_Money_In_Wallet ? Add_Money_In_Wallet : Got_Reward

            const fetch_Last_Lap_Wallet = findPackage[i].lapWalletDetail;

            let check = LapWalletArr.length == 0 ? -1 : LapWalletArr.findIndex((value) => value.BonusOwner === findPackage[i].PackageOwner)
            let checkUpdate = UpdateLapWalletArr.length == 0 ? -1 : UpdateLapWalletArr.findIndex((value) => value.updateOne.filter.BonusOwner === findPackage[i].PackageOwner)

            if (check == -1 && !fetch_Last_Lap_Wallet) {
              LapWalletArr.push({
                BonusOwner: findPackage[i].PackageOwner,
                LapAmount: Lap_Income
              })
            } else if (checkUpdate == -1 && fetch_Last_Lap_Wallet) {
              UpdateLapWalletArr?.push({
                "updateOne": {
                  "filter": { "BonusOwner": findPackage[i].PackageOwner },
                  "update": { $set: { "LapAmount": Lap_Income + fetch_Last_Lap_Wallet.LapAmount } }
                }
              })
            } else {
              if (fetch_Last_Lap_Wallet) {
                UpdateLapWalletArr[checkUpdate].updateOne.update.$set.LapAmount += Lap_Income
              } else {
                let sum = Number(Lap_Income) + Number(LapWalletArr[check].LapAmount)
                LapWalletArr[check].LapAmount = sum
              }
            }
          }


          if (Add_Money_In_Wallet > 0) {
            if(per == 0.3){    
              // console.log("DailyBonusArr 111111111 ============= ", Add_Money_In_Wallet, findPackage[i].PackageOwner)
              DailyBonusArr.push({
                BonusOwner: findPackage[i].id,
                FormPackage: findPackage[i].PackageName,
                PackagePercantage: per,
                Amount: Add_Money_In_Wallet
              })
            } else {
              // console.log("LykaFastBonusHisArr 1111111111 ============= ", Add_Money_In_Wallet, findPackage[i].PackageOwner)
              LykaFastBonusHisArr.push({
                BonusOwner: findPackage[i].id,
                FormPackage: findPackage[i].PackageName,
                PackagePercantage: per,
                Amount: Add_Money_In_Wallet
              })
            }

            // let checkUpdate = UpdateUserDetailArr.length == 0 ? -1 : UpdateUserDetailArr.findIndex((value) => value.updateOne.filter._id.toString() == findPackage[i].PackageOwner)

            // if(checkUpdate == -1){
            //   UpdateUserDetailArr?.push({
            //     "updateOne": {
            //       "filter": { "_id": findPackage[i].PackageOwner },
            //       "update": { $set: { "MainWallet": Number(myWallete) + Number(Add_Money_In_Wallet) } }
            //     }
            //   })
            // } else {
            //   UpdateUserDetailArr[checkUpdate].updateOne.update.$set.MainWallet += Number(Add_Money_In_Wallet)
            // }
          }
        } else {
          Add_Money_In_Wallet = Got_Reward

          // console.log("Add_Money_In_Wallet ============= ", Add_Money_In_Wallet, findPackage[i].PackageOwner)
          if (Add_Money_In_Wallet > 0) {
            if(per == 0.3){    
              // console.log("DailyBonusArr 22222222 ============= ", Add_Money_In_Wallet, findPackage[i].PackageOwner)
              DailyBonusArr.push({
                BonusOwner: findPackage[i].PackageOwner,
                FormPackage: findPackage[i].PackageName,
                PackagePercantage: per,
                Amount: Got_Reward
              })
            } else {
              // console.log("LykaFastBonusHisArr 22222222 ============= ", Add_Money_In_Wallet, findPackage[i].PackageOwner)
              LykaFastBonusHisArr.push({
                BonusOwner: findPackage[i].PackageOwner,
                FormPackage: findPackage[i].PackageName,
                PackagePercantage: per,
                Amount: Got_Reward
              })
            }

            // let checkUpdate = UpdateUserDetailArr.length == 0 ? -1 : UpdateUserDetailArr.findIndex((value) => value.updateOne.filter._id.toString() == findPackage[i].PackageOwner)

            // if(checkUpdate == -1){
            //   UpdateUserDetailArr?.push({
            //     "updateOne": {
            //       "filter": { "_id": findPackage[i].PackageOwner },
            //       "update": { $set: { "MainWallet": Number(myWallete) + Number(Add_Money_In_Wallet) } }
            //     }
            //   })
            // } else {
            //   UpdateUserDetailArr[checkUpdate].updateOne.update.$set.MainWallet += Number(Add_Money_In_Wallet)
            // }
          }
        }

        let checkUpdate = UpdateUserDetailArr.length == 0 ? -1 : UpdateUserDetailArr.findIndex((value) => value.updateOne.filter._id.toString() == findPackage[i].PackageOwner)

        if(checkUpdate == -1){
          UpdateUserDetailArr?.push({
            "updateOne": {
              "filter": { "_id": findPackage[i].PackageOwner },
              "update": { $set: { "MainWallet": myWallete + Add_Money_In_Wallet } }
            }
          })
        } else {
          UpdateUserDetailArr[checkUpdate].updateOne.update.$set.MainWallet += Add_Money_In_Wallet
        }

        const findShortRecord = findPackage[i].shortRecordDetail;
        let check = ShortRecordArr.length == 0 ? -1 : ShortRecordArr.findIndex((value) => value.RecordOwner === findPackage[i].PackageOwner)
        let checkInUpdate = UpdateShortRecordArr.length == 0 ? -1 : UpdateShortRecordArr.findIndex((value) => value?.updateOne.filter.RecordOwner === findPackage[i].PackageOwner)
        let fieldValue = "PowerStaing";

        if(per == 0.3){ 
          fieldValue = "DailyStakig";
        }

        if (check == -1 && !findShortRecord) {
          if(per == 0.3){
            ShortRecordArr.push({
              RecordOwner: findPackage[i].PackageOwner,
              DailyStakig: Add_Money_In_Wallet
            })
          } else {
            ShortRecordArr.push({
              RecordOwner: findPackage[i].PackageOwner,
              PowerStaing: Add_Money_In_Wallet
            })
          }
        } else if (checkInUpdate == -1 && findShortRecord) {
          let sum = Number(findShortRecord[fieldValue]) + Number(Add_Money_In_Wallet)
          if(per == 0.3){
            UpdateShortRecordArr?.push({
              "updateOne": {
                "filter": { "RecordOwner": findPackage[i].PackageOwner },
                "update": { $set: { "DailyStakig": sum } }
              }
            })
          } else {
            UpdateShortRecordArr?.push({
              "updateOne": {
                "filter": { "RecordOwner": findPackage[i].PackageOwner },
                "update": { $set: { "PowerStaing": sum } }
              }
            })
          }
        } else {
          if (findShortRecord) {
            let total = 0;
            if(UpdateShortRecordArr[checkInUpdate] && UpdateShortRecordArr[checkInUpdate].updateOne.update.$set[fieldValue] === undefined) {
              total = Number(findShortRecord[fieldValue]) + Number(Add_Money_In_Wallet);
              UpdateShortRecordArr[checkInUpdate].updateOne.update.$set[fieldValue] = 0;
            } else {
              total = Number(Add_Money_In_Wallet);
            }
            UpdateShortRecordArr[checkInUpdate]["updateOne"]["update"]["$set"][fieldValue] += parseFloat(total)
          } else {
            let sum = Number(ShortRecordArr[check][fieldValue]) + Number(Add_Money_In_Wallet)
            ShortRecordArr[check][fieldValue] = sum
          }
        }
      } else {
        const FindPackage = findPackage[i];

        const Max_Cap = Number(FindPackage.PackagePrice) * 300 / 100
        const Got_Reward = Number(finalCal)
        const My_Wallet = Number(myOldWallet.MainWallet)
        var Add_Money_In_Wallet = 0;

        if (Got_Reward + My_Wallet >= Max_Cap) {
          Add_Money_In_Wallet = Max_Cap - My_Wallet
          if(Add_Money_In_Wallet>0){
            const Lap_Income = Got_Reward > Add_Money_In_Wallet ? Add_Money_In_Wallet : Got_Reward

            const fetch_Last_Lap_Wallet = findPackage[i].lapWalletDetail;

            let check = LapWalletArr.length == 0 ? -1 : LapWalletArr.findIndex((value) => value.BonusOwner === findPackage[i].PackageOwner)
            let checkUpdate = UpdateLapWalletArr.length == 0 ? -1 : UpdateLapWalletArr.findIndex((value) => value.updateOne.filter.BonusOwner === findPackage[i].PackageOwner)
            // console.log("checkUpdate ============= ", checkUpdate)

            if (check == -1 && !fetch_Last_Lap_Wallet) {
              LapWalletArr.push({
                BonusOwner: findPackage[i].PackageOwner,
                LapAmount: Lap_Income
              })
            } else if (checkUpdate == -1 && fetch_Last_Lap_Wallet) {
              UpdateLapWalletArr?.push({
                "updateOne": {
                  "filter": { "BonusOwner": findPackage[i].PackageOwner },
                  "update": { $set: { "LapAmount": Lap_Income + fetch_Last_Lap_Wallet.LapAmount } }
                }
              })
            } else {
              if (fetch_Last_Lap_Wallet) {
                UpdateLapWalletArr[checkUpdate].updateOne.update.$set.LapAmount += Lap_Income
              } else {
                let sum = Number(Lap_Income) + Number(LapWalletArr[check].LapAmount)
                LapWalletArr[check].LapAmount = sum
              }
            }
          }

          if (Add_Money_In_Wallet > 0) {
            // console.log("DailyBonusArr 33333333 ============= ", Add_Money_In_Wallet, findPackage[i].PackageOwner)
            console.log("coming here")
            
            if(per == 0.3){    
              // console.log("DailyBonusArr 111111111 ============= ", Add_Money_In_Wallet, findPackage[i].PackageOwner)
              DailyBonusArr.push({
                BonusOwner: findPackage[i].PackageOwner,
                FormPackage: findPackage[i].PackageName,
                PackagePercantage: per,
                Amount: Add_Money_In_Wallet
              })
            } else {
              // console.log("LykaFastBonusHisArr 1111111111 ============= ", Add_Money_In_Wallet, findPackage[i].PackageOwner)
              LykaFastBonusHisArr.push({
                BonusOwner: findPackage[i].PackageOwner,
                FormPackage: findPackage[i].PackageName,
                PackagePercantage: per,
                Amount: Add_Money_In_Wallet
              })
            }
            // DailyBonusArr.push({
            //   BonusOwner: findPackage[i].PackageOwner,
            //   FormPackage: findPackage[i].PackageName,
            //   PackagePercantage: per,
            //   Amount: Add_Money_In_Wallet
            // })

            const findShortRecord = findPackage[i].shortRecordDetail;
            let check = ShortRecordArr.length == 0 ? -1 : ShortRecordArr.findIndex((value) => value.RecordOwner === findPackage[i].PackageOwner)
            let checkInUpdate = UpdateShortRecordArr.length == 0 ? -1 : UpdateShortRecordArr.findIndex((value) => value?.updateOne.filter.RecordOwner === findPackage[i].PackageOwner)

            if (check == -1 && !findShortRecord) {
              ShortRecordArr.push({
                RecordOwner: findPackage[i].PackageOwner,
                DailyStakig: Number(Add_Money_In_Wallet)
              })
            } else if (checkInUpdate == -1 && findShortRecord) {
              let sum = Number(findShortRecord.DailyStakig) + Number(Add_Money_In_Wallet)
              UpdateShortRecordArr?.push({
                "updateOne": {
                  "filter": { "RecordOwner": findPackage[i].PackageOwner },
                  "update": { $set: { "DailyStakig": sum } }
                }
              })
            } else {
              if (findShortRecord) {
                let total = 0;
                if(UpdateShortRecordArr[checkInUpdate].updateOne.update.$set["DailyStakig"] === undefined) {
                  total = Number(findShortRecord.DailyStakig) + Number(Add_Money_In_Wallet);
                  UpdateShortRecordArr[checkInUpdate].updateOne.update.$set.DailyStakig = 0;
                } else {
                  total = Number(Add_Money_In_Wallet);
                }
                UpdateShortRecordArr[checkInUpdate].updateOne.update.$set.DailyStakig += total
              } else {
                let sum = Number(ShortRecordArr[check].DailyStakig) + Number(Add_Money_In_Wallet)
                ShortRecordArr[check].DailyStakig = sum
              }
            }
          }
        } else {
          Add_Money_In_Wallet = Got_Reward
          // console.log("DailyBonusArr 444444444 ============= ", Add_Money_In_Wallet, findPackage[i].PackageOwner)
          if(per == 0.3){    
            // console.log("DailyBonusArr 111111111 ============= ", Add_Money_In_Wallet, findPackage[i].PackageOwner)
            DailyBonusArr.push({
              BonusOwner: findPackage[i].PackageOwner,
              FormPackage: findPackage[i].PackageName,
              PackagePercantage: per,
              Amount: Got_Reward
            })
          } else {
            // console.log("LykaFastBonusHisArr 1111111111 ============= ", Add_Money_In_Wallet, findPackage[i].PackageOwner)
            LykaFastBonusHisArr.push({
              BonusOwner: findPackage[i].PackageOwner,
              FormPackage: findPackage[i].PackageName,
              PackagePercantage: per,
              Amount: Got_Reward
            })
          }
          // DailyBonusArr.push({
          //   BonusOwner: findPackage[i].PackageOwner,
          //   FormPackage: findPackage[i].PackageName,
          //   PackagePercantage: per,
          //   Amount: Got_Reward
          // })
        }
      }
    } else {
      if (per>=1) {
        const FindPackage = findPackage[i];

        const Max_Cap = Number(FindPackage.PackagePrice) * 300 / 100
        const Got_Reward = Number(finalCal)
        const My_Wallet = Number(myOldWallet.MainWallet)
        var Add_Money_In_Wallet = 0;

        if (Got_Reward + My_Wallet >= Max_Cap) {
          Add_Money_In_Wallet = Max_Cap - My_Wallet
          if(Add_Money_In_Wallet>0){
            const Lap_Income = Got_Reward > Add_Money_In_Wallet ? Add_Money_In_Wallet : Got_Reward


            const fetch_Last_Lap_Wallet = findPackage[i].lapWalletDetail;

            let check = LapWalletArr.length == 0 ? -1 : LapWalletArr.findIndex((value) => value.BonusOwner === findPackage[i].PackageOwner)
            let checkUpdate = UpdateLapWalletArr.length == 0 ? -1 : UpdateLapWalletArr.findIndex((value) => value.updateOne.filter.BonusOwner === findPackage[i].PackageOwner)

            if (check == -1 && !fetch_Last_Lap_Wallet) {
              LapWalletArr.push({
                BonusOwner: findPackage[i].PackageOwner,
                LapAmount: Lap_Income
              })
            } else if (checkUpdate == -1 && fetch_Last_Lap_Wallet) {
              UpdateLapWalletArr?.push({
                "updateOne": {
                  "filter": { "BonusOwner": findPackage[i].PackageOwner },
                  "update": { $set: { "LapAmount": Lap_Income + fetch_Last_Lap_Wallet.LapAmount } }
                }
              })
            } else {
              if (fetch_Last_Lap_Wallet) {
                UpdateLapWalletArr[checkUpdate].updateOne.update.$set.LapAmount += Lap_Income
              } else {
                let sum = Number(Lap_Income) + Number(LapWalletArr[check].LapAmount)
                LapWalletArr[check].LapAmount = sum
              }
            }
          }
          if (Add_Money_In_Wallet > 0) {
            if(per == 0.3){    
              // console.log("DailyBonusArr 55555555 ============= ", Add_Money_In_Wallet, findPackage[i].PackageOwner)
              DailyBonusArr.push({
                BonusOwner: findPackage[i].PackageOwner,
                FormPackage: findPackage[i].PackageName,
                PackagePercantage: per,
                Amount: Add_Money_In_Wallet
              })
            } else {
              // console.log("LykaFastBonusHisArr 33333333 ============= ", Add_Money_In_Wallet, findPackage[i].PackageOwner)
              LykaFastBonusHisArr.push({
                BonusOwner: findPackage[i].PackageOwner,
                FormPackage: findPackage[i].PackageName,
                PackagePercantage: per,
                Amount: Add_Money_In_Wallet
              })
            }

            let checkUpdate = UpdateUserDetailArr.length == 0 ? -1 : UpdateUserDetailArr.findIndex((value) => value.updateOne.filter._id.toString() == findPackage[i].PackageOwner)

            if(checkUpdate == -1){
              UpdateUserDetailArr?.push({
                "updateOne": {
                  "filter": { "_id": findPackage[i].PackageOwner },
                  "update": { $set: { "MainWallet": Number(myWallete) + Number(Add_Money_In_Wallet) } }
                }
              })
            } else {
              UpdateUserDetailArr[checkUpdate].updateOne.update.$set.MainWallet += Number(Add_Money_In_Wallet)
            }
          }
        } else {
          Add_Money_In_Wallet = Got_Reward
          if (Add_Money_In_Wallet > 0) {
            if(per == 0.3){    
              // console.log("DailyBonusArr 666666666 ============= ", Add_Money_In_Wallet, findPackage[i].PackageOwner)
              DailyBonusArr.push({
                BonusOwner: findPackage[i].PackageOwner,
                FormPackage: findPackage[i].PackageName,
                PackagePercantage: per,
                Amount: Got_Reward
              })
            } else {
              // console.log("LykaFastBonusHisArr 444444444 ============= ", Add_Money_In_Wallet, findPackage[i].PackageOwner)
              LykaFastBonusHisArr.push({
                BonusOwner: findPackage[i].PackageOwner,
                FormPackage: findPackage[i].PackageName,
                PackagePercantage: per,
                Amount: Got_Reward
              })
            }

            let checkUpdate = UpdateUserDetailArr.length == 0 ? -1 : UpdateUserDetailArr.findIndex((value) => value.updateOne.filter._id.toString() == findPackage[i].PackageOwner)

            if(checkUpdate == -1){
              UpdateUserDetailArr?.push({
                "updateOne": {
                  "filter": { "_id": findPackage[i].PackageOwner },
                  "update": { $set: { "MainWallet": Number(myWallete) + Number(Got_Reward) } }
                }
              })
            } else {
              UpdateUserDetailArr[checkUpdate].updateOne.update.$set.MainWallet += Number(Got_Reward)
            }
          }
        }

        const findShortRecord = findPackage[i].shortRecordDetail;
        let check = ShortRecordArr.length == 0 ? -1 : ShortRecordArr.findIndex((value) => value.RecordOwner === findPackage[i].PackageOwner)
        let checkInUpdate = UpdateShortRecordArr.length == 0 ? -1 : UpdateShortRecordArr.findIndex((value) => value?.updateOne.filter.RecordOwner === findPackage[i].PackageOwner)
        let fieldValue = "PowerStaing";

        if(per == 0.3){ 
          fieldValue = "DailyStakig";
        }

        if (check == -1 && !findShortRecord) {
          if(per == 0.3){  
            ShortRecordArr.push({
              RecordOwner: findPackage[i].PackageOwner,
              DailyStakig: Add_Money_In_Wallet
            })
          } else {
            ShortRecordArr.push({
              RecordOwner: findPackage[i].PackageOwner,
              PowerStaing: Add_Money_In_Wallet
            })
          }
        } else if (checkInUpdate == -1 && findShortRecord) {
          let sum = Number(findShortRecord[fieldValue]) + Number(Add_Money_In_Wallet)
          if(per == 0.3){ 
            UpdateShortRecordArr?.push({
              "updateOne": {
                "filter": { "RecordOwner": findPackage[i].PackageOwner },
                "update": { $set: { "DailyStakig": sum } }
              }
            })
          } else {
            UpdateShortRecordArr?.push({
              "updateOne": {
                "filter": { "RecordOwner": findPackage[i].PackageOwner },
                "update": { $set: { "PowerStaing": sum } }
              }
            })
          }
        } else {
          if (findShortRecord) {
            let total = 0;
            if(UpdateShortRecordArr[checkInUpdate].updateOne.update.$set[fieldValue] === undefined) {
              total = Number(findShortRecord[fieldValue]) + Number(Add_Money_In_Wallet);
              UpdateShortRecordArr[checkInUpdate].updateOne.update.$set[fieldValue] = 0;
            } else {
              total = Number(Add_Money_In_Wallet);
            }
            UpdateShortRecordArr[checkInUpdate].updateOne.update.$set[fieldValue] += parseFloat(total)
          } else {
            let sum = Number(ShortRecordArr[check][fieldValue]) + Number(Add_Money_In_Wallet)
            ShortRecordArr[check][fieldValue] = sum
          }
        }
      } else {
        const FindPackage = findPackage[i]

        const Max_Cap = Number(FindPackage.PackagePrice) * 300 / 100
        const Got_Reward = Number(finalCal)
        const My_Wallet = Number(myOldWallet.MainWallet)
        var Add_Money_In_Wallet = 0;

        if (Got_Reward + My_Wallet >= Max_Cap) {
          Add_Money_In_Wallet = Max_Cap - My_Wallet
          if (Add_Money_In_Wallet>0) {
            const Lap_Income = parseInt(Got_Reward) > parseInt(Add_Money_In_Wallet) ? parseInt(Add_Money_In_Wallet) : parseInt(Got_Reward)

            const fetch_Last_Lap_Wallet = findPackage[i].lapWalletDetail;

            let check = LapWalletArr.length == 0 ? -1 : LapWalletArr.findIndex((value) => value.BonusOwner === findPackage[i].PackageOwner)
            let checkUpdate = UpdateLapWalletArr.length == 0 ? -1 : UpdateLapWalletArr.findIndex((value) => value.updateOne.filter.BonusOwner === findPackage[i].PackageOwner)

            if (check == -1 && !fetch_Last_Lap_Wallet) {
              LapWalletArr.push({
                BonusOwner: findPackage[i].PackageOwner,
                LapAmount: Lap_Income
              })
            } else if (checkUpdate == -1 && fetch_Last_Lap_Wallet) {
              UpdateLapWalletArr?.push({
                "updateOne": {
                  "filter": { "BonusOwner": findPackage[i].PackageOwner },
                  "update": { $set: { "LapAmount": Lap_Income + fetch_Last_Lap_Wallet.LapAmount } }
                }
              })
            } else {
              if (fetch_Last_Lap_Wallet) {
                UpdateLapWalletArr[checkUpdate].updateOne.update.$set.LapAmount += Lap_Income
              } else {
                let sum = Number(Lap_Income) + Number(LapWalletArr[check].LapAmount)
                LapWalletArr[check].LapAmount = sum
              }
            }
          }

          if (Add_Money_In_Wallet > 0) {
            // console.log("DailyBonusArr 777777777 ============= ", Add_Money_In_Wallet, findPackage[i].PackageOwner)
            if(per == 0.3){    
              // console.log("DailyBonusArr 111111111 ============= ", Add_Money_In_Wallet, findPackage[i].PackageOwner)
              DailyBonusArr.push({
                BonusOwner: findPackage[i].PackageOwner,
                FormPackage: findPackage[i].PackageName,
                PackagePercantage: per,
                Amount: Add_Money_In_Wallet
              })
            } else {
              // console.log("LykaFastBonusHisArr 1111111111 ============= ", Add_Money_In_Wallet, findPackage[i].PackageOwner)
              LykaFastBonusHisArr.push({
                BonusOwner: findPackage[i].PackageOwner,
                FormPackage: findPackage[i].PackageName,
                PackagePercantage: per,
                Amount: Add_Money_In_Wallet
              })
            }
            // DailyBonusArr.push({
            //   BonusOwner: findPackage[i].PackageOwner,
            //   FormPackage: findPackage[i].PackageName,
            //   PackagePercantage: per,
            //   Amount: Add_Money_In_Wallet
            // })
          }
        } else {
          Add_Money_In_Wallet = Got_Reward
          // console.log("DailyBonusArr 888888888 ============= ", Add_Money_In_Wallet, findPackage[i].PackageOwner)
          if(per == 0.3){    
            // console.log("DailyBonusArr 111111111 ============= ", Add_Money_In_Wallet, findPackage[i].PackageOwner)
            DailyBonusArr.push({
              BonusOwner: findPackage[i].PackageOwner,
              FormPackage: findPackage[i].PackageName,
              PackagePercantage: per,
              Amount: Got_Reward
            })
          } else {
            // console.log("LykaFastBonusHisArr 1111111111 ============= ", Add_Money_In_Wallet, findPackage[i].PackageOwner)
            LykaFastBonusHisArr.push({
              BonusOwner: findPackage[i].PackageOwner,
              FormPackage: findPackage[i].PackageName,
              PackagePercantage: per,
              Amount: Got_Reward
            })
          }
          // DailyBonusArr.push({
          //   BonusOwner: findPackage[i].PackageOwner,
          //   FormPackage: findPackage[i].PackageName,
          //   PackagePercantage: per,
          //   Amount: Got_Reward
          // })
        }
        if(Add_Money_In_Wallet > 0){    
          let checkUpdate = UpdateUserDetailArr.length == 0 ? -1 : UpdateUserDetailArr.findIndex((value) => value.updateOne.filter._id.toString() == findPackage[i].PackageOwner)

          if(checkUpdate == -1){
            UpdateUserDetailArr?.push({
              "updateOne": {
                "filter": { "_id": findPackage[i].PackageOwner },
                "update": { $set: { "MainWallet": Number(myWallete) + Number(Add_Money_In_Wallet) } }
              }
            })
          } else {
            UpdateUserDetailArr[checkUpdate].updateOne.update.$set.MainWallet += Number(Add_Money_In_Wallet)
          }

          const findShortRecord = findPackage[i].shortRecordDetail;
          let check = ShortRecordArr.length == 0 ? -1 : ShortRecordArr.findIndex((value) => value.RecordOwner === findPackage[i].PackageOwner)
          let checkInUpdate = UpdateShortRecordArr.length == 0 ? -1 : UpdateShortRecordArr.findIndex((value) => value?.updateOne.filter.RecordOwner === findPackage[i].PackageOwner)

          if (check == -1 && !findShortRecord) {
            ShortRecordArr.push({
              RecordOwner: findPackage[i].PackageOwner,
              DailyStakig: Add_Money_In_Wallet
            })
          } else if (checkInUpdate == -1 && findShortRecord) {
            let sum = Number(findShortRecord.DailyStakig) + Number(Add_Money_In_Wallet)
            UpdateShortRecordArr?.push({
              "updateOne": {
                "filter": { "RecordOwner": findPackage[i].PackageOwner },
                "update": { $set: { "DailyStakig": sum } }
              }
            })
          } else {
            if (findShortRecord) {
              let total = 0;
              if(UpdateShortRecordArr[checkInUpdate].updateOne.update.$set["DailyStakig"] === undefined) {
                total = Number(findShortRecord.DailyStakig) + Number(Add_Money_In_Wallet);
                UpdateShortRecordArr[checkInUpdate].updateOne.update.$set.DailyStakig = 0;
              } else {
                total = Number(Add_Money_In_Wallet);
              }
              UpdateShortRecordArr[checkInUpdate].updateOne.update.$set.DailyStakig += parseFloat(total)
            } else {
              let sum = Number(ShortRecordArr[check].DailyStakig) + Number(Add_Money_In_Wallet)
              ShortRecordArr[check].DailyStakig = sum
            }
          }
        }
      }
    }
  }

  // console.log(ShortRecordArr, "3")
  // console.log(JSON.stringify(UpdateUserDetailArr), "3")

  console.log(ShortRecordArr)


  await LapWallet.insertMany(LapWalletArr)
  await RebuyBonus.insertMany(RebuyBonusArr)
  await ShortRecord.insertMany(ShortRecordArr)
  await LykaFastBonusHis.insertMany(LykaFastBonusHisArr)
  await DailyBonus.insertMany(DailyBonusArr)
  await ShortRecord.bulkWrite(UpdateShortRecordArr);
  await LapWallet.bulkWrite(UpdateLapWalletArr);
  await User.bulkWrite(UpdateUserDetailArr);

  res.json({})
}