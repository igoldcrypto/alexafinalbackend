const PackageHistory = require("../Models/History/PackageHistory");
const PackageInvoice = require("../Models/Invoice/PurchasePackageInvoice");
const User = require("../Models/User");
const MatchingBonusHistory = require("../Models/History/MatchingBonusHistory");
const ShortRecord = require("../Models/ShortRecord");
const moment = require("moment");
const PurchasePackageInvoice = require("../Models/Invoice/PurchasePackageInvoice");

exports.NewMatchingBonus = async (req, res) => {

  var updateOps = [];

  const Matching_Bonus_History_Array = [];

  let totalBussinessCache = {};

  const [
    PackageHistorys,
    Users,
    MatchingBonusHistorys,
    FiveMinutesInvoices,
    ShortRecords,
    PackageInvoices,
  ] = await Promise.all([
    PackageHistory.find().lean(),
    User.find().lean(),
    MatchingBonusHistory.find().lean(),
    PurchasePackageInvoice.find({
      createdAt: {
        $gte: moment().subtract(10080, "minutes").toDate(),
        $lt: moment().toDate(),
      },
    }).lean(),
    ShortRecord.find().lean(),
    PackageInvoice.find().lean(),
  ]);

  const findTotalBussiness = (userId, totalBussinessCache) => {


    if (userId === undefined || userId === "null") {
      return {
        success: true,
        data: {
          leftIncome: 0,
          rightIncome: 0,
          totalIncome: 0,
        },
      };
    }

    if (totalBussinessCache[userId] !== undefined)
      return {
        success: true,
        data: totalBussinessCache[userId],
      };


    try {
      let currentUser = Users.find(
        (e) => e._id.toString() == userId.toString()
      );

      const currentUserInvoices = FiveMinutesInvoices.filter(
        (currentInvoice) =>
          currentInvoice?.PackageOwner?.toString() === currentUser?._id?.toString()
      );

      let totalAmount = 0;

      currentUserInvoices.forEach(
        (currentInvoice) => (totalAmount += Number(currentInvoice.PackagePrice))
      );

      let leftUserId = currentUser?.LeftTeamId;
      let rightUserId = currentUser?.RightTeamId;

      const leftIncome = findTotalBussiness(leftUserId, totalBussinessCache);
      if (!leftIncome.success) return leftIncome;

      const rightIncome = findTotalBussiness(rightUserId, totalBussinessCache);
      if (!rightIncome.success) return rightIncome;




      const returningIncome = {
        leftIncome: leftIncome.data.totalIncome,
        rightIncome: rightIncome.data.totalIncome,
        totalIncome:
          leftIncome.data.totalIncome +
          rightIncome.data.totalIncome +
          totalAmount,
      };

      totalBussinessCache[userId] = returningIncome;

      return {
        success: true,
        data: returningIncome,
      };
    } catch (error) {
      if (error instanceof Error || error instanceof MongoServerError) {
        return {
          success: false,
          error: error.message,
        };
      }

      return {
        success: false,
        error: "Internal Server Error",
      };
    }
  };







  for (let index = 0; index < Users.length; index++) {
    const User_Item = Users[index]._id;


    const Check_If_User_Already_Own_Any_Matching_Bonus =
      MatchingBonusHistorys.filter((e) => e.BonusOwner == User_Item.toString());

    const Find_If_User_Have_Package = PackageHistorys.filter(
      (e) => e.PackageOwner == User_Item.toString()
    );

    if (Find_If_User_Have_Package.length == 0) continue;

    if (
      Find_If_User_Have_Package.length > 0 &&
      Find_If_User_Have_Package[0].Type == "Repurchased"
    ) {
      //  // 

      if (Check_If_User_Already_Own_Any_Matching_Bonus.length > 0) {
        const getArrayLenght =
          Check_If_User_Already_Own_Any_Matching_Bonus.length;

        var SelectSide =
          Check_If_User_Already_Own_Any_Matching_Bonus[
            getArrayLenght - 1 < 0 ? 0 : getArrayLenght - 1
          ].SubtractedFrom;
        var subLastValue = Number(
          Check_If_User_Already_Own_Any_Matching_Bonus[
            getArrayLenght - 1 < 0 ? 0 : getArrayLenght - 1
          ].ForwardedValue
        );
      } else {
        var SelectSide = "Left";
        var subLastValue = 0;
      }

      let User_Purchased_Package_Type3 = Find_If_User_Have_Package[0].Type;

      let Package_Price = Find_If_User_Have_Package[0].PackagePrice;

      const Find_User_Directs = await User.find({
        UpperlineUser: User_Item._id,
      });


      if (Find_User_Directs.length !== 0) {
        var LeftWall = 0;
        var LeftWallId = "";
        var RightWall = 0;
        var RightWallId = "";

        for (let index = 0; index < Find_User_Directs.length; index++) {
          const Direct_User_Element = Find_User_Directs[index].Position;

          if (Direct_User_Element == "Right") {
            LeftWall =
              LeftWall + Number(Find_User_Directs[index].PurchasedPackagePrice);
            LeftWallId = Find_User_Directs[index]._id;
          }
          if (Direct_User_Element == "Left") {
            RightWall =
              RightWall +
              Number(Find_User_Directs[index].PurchasedPackagePrice);
            RightWallId = Find_User_Directs[index]._id;
          }
        }

        //// 
        //// 
        ////  

        if (
          LeftWall >= Number(Package_Price) &&
          RightWall >= Number(Package_Price)
        ) {
          //  // 

          var Latest_Left_Value_After_Deduct = 0;
          var Latest_Right_Value_After_Deduct = 0;

          if (User_Purchased_Package_Type3 == "Repurchased") {
            //// 

            let Amount_I_Need_To_Minus = 0;

            const Find_My_Repurchase = PackageInvoices.filter(
              (e) => e.PackageOwner.toString() == User_Item._id
            );

            let Total_Num = Find_My_Repurchase.length;

            Find_My_Repurchase.map((hit, index) => {
              if (index == Total_Num - 1) {
                return;
              }
              return (Amount_I_Need_To_Minus =
                Amount_I_Need_To_Minus + Number(hit.PackagePrice));
            });
            // 
            // 

            if (Amount_I_Need_To_Minus > LeftWall || Amount_I_Need_To_Minus > RightWall) {
              console.log(
                `Breaking This Loop For This => ${Find_If_User_Have_Package[0].PackageOwner} Because he want to deuduct ${Amount_I_Need_To_Minus} but in Left he has ${LeftWall} and in right side he has ${RightWall}.`
              );
              continue;
            }

            Latest_Left_Value_After_Deduct = LeftWall - Amount_I_Need_To_Minus;

            Latest_Right_Value_After_Deduct =
              RightWall - Amount_I_Need_To_Minus;

            // console.log(
            //   "Latest_Left_Value_After_Deduct => " +
            //   Latest_Left_Value_After_Deduct
            // );
            // console.log(
            //   "Latest_Right_Value_After_Deduct => " +
            //   Latest_Right_Value_After_Deduct
            // );

            if (
              Latest_Left_Value_After_Deduct <= 0 ||
              Latest_Right_Value_After_Deduct <= 0
            ) {
              console.log(
                `Breaking This Loop because ${Find_If_User_Have_Package[0].PackageOwner} Latest_Left_Value_After_Deduct is ${Latest_Left_Value_After_Deduct} or ${Latest_Right_Value_After_Deduct} is less/euals to 0.`
              );
              continue;
            }

            await PackageHistory.findByIdAndUpdate(
              { _id: Find_If_User_Have_Package[0]._id },
              { Type3: "Basic" }
            );

            // 
          } else {
            // 
          }



          if (
            Latest_Left_Value_After_Deduct >= Number(Package_Price) &&
            Latest_Right_Value_After_Deduct >= Number(Package_Price)
          ) {
            // 


            const currentUserBussiness = await findTotalBussiness(
              User_Item,
              totalBussinessCache
            );




            let leftBusiness = currentUserBussiness.data.leftIncome;
            let rightBusiness = currentUserBussiness.data.rightIncome;

            
            


            if (SelectSide == "Left") {
              leftBusiness = Number(leftBusiness) + Number(subLastValue);
            } else {
              rightBusiness = Number(rightBusiness) + Number(subLastValue);
            }
            

            console.log("----------------------------------------------------------------------")
            console.log("Left From Binary => " + leftBusiness)
            console.log("rightBusiness From Binary => " + rightBusiness)
            console.log(User_Item)
            console.log("----------------------------------------------------------------------")



            if (
              1 == 1
            ) {
              var combo = 0;

              if (leftBusiness < rightBusiness) {
                combo = Number(leftBusiness);
                var subtractForwardValue = rightBusiness - leftBusiness;
                var subtracted_From_Which_Side = "Right";
              } else if (rightBusiness < leftBusiness) {
                combo = Number(rightBusiness);
                var subtractForwardValue = leftBusiness - rightBusiness;
                var subtracted_From_Which_Side = "Left";
              } else if (rightBusiness == leftBusiness) {
                combo = Number(rightBusiness);
                var subtractForwardValue = 0;
                var subtracted_From_Which_Side = "Left";
              }

              /*
              ! FIND SHORT RECORD FOR THIS USER
              */

              const Find_Short_Record = ShortRecords.filter(
                (e) => e.RecordOwner.toString() == User_Item
              );

              const FinalShort = Find_Short_Record[0]

              var packPercantage = (Number(combo) * 8) / 100;

              const GiveMatchingBonus = Users.filter(
                (e) => e._id == User_Item.toString()
              );

              /*
                            *! GOING TO CALCULATE MAX CAPING FOR THIS USER
                            ! FORMULA ==> MAX I CAN EARN = 300
                            !             CURRENT WALLET = 280 
                            !             NEXT I WILL GET REWARD = 50 
                            !         let Value = MAX I CAN EARN  - CURRENT WALLET
                            !         let Reward = NEXT I WILL GET REWARD > Value ? Value : NEXT I WILL GET REWARD
                            */


              /*
              ! GET MY ALL EARNINGS
              */

              const All_My_Daily_Income = FinalShort ? FinalShort.DailyStakig : 0
              const All_My_Power_Income = FinalShort ? FinalShort.PowerStaing : 0
              const All_My_Direct_Reward = FinalShort ? FinalShort.DirectReward : 0
              const All_My_Matcing_Bonus = FinalShort ? FinalShort.MatcingBonus : 0
              const All_My_Rank_Eligibility = FinalShort ? FinalShort.RankEligibility : 0
              const All_My_Gobal_Pool_Bonus = FinalShort ? FinalShort.GobalPoolBonus : 0
              const All_My_Rebuy_Bonus = FinalShort ? FinalShort.RebuyBonus : 0

              const Total_Earning = All_My_Daily_Income + All_My_Power_Income + All_My_Direct_Reward + All_My_Matcing_Bonus + All_My_Rank_Eligibility + All_My_Gobal_Pool_Bonus + All_My_Rebuy_Bonus






              /*
              ! GOES TILL HERE 
              */


              const Max_I_Can_Earn = (Number(Package_Price) * 300) / 100; // Max I can earn

              const My_Current_Walet = Total_Earning

              const Future_I_Will_Get_Reward = packPercantage;

              let Reward = Max_I_Can_Earn - Users[index].MainWallet;

              console.log(Reward)
              let Final_Reward = Future_I_Will_Get_Reward > Reward ? Reward : Future_I_Will_Get_Reward;

              // const userWallet = Number(GiveMatchingBonus[0].MainWallet) + Number(Final_Reward);


              // MAX CAPING DONE

              var I_Can_Maximum_Get = Number(Package_Price) * 300 / 100

              var Flushed_Data = 0

              if (Number(Final_Reward) > I_Can_Maximum_Get) {
                Final_Reward = I_Can_Maximum_Get
                Flushed_Data = Number(Final_Reward) - I_Can_Maximum_Get
              }


              // console.log("first one")
              if (Final_Reward >= 0) {

                Matching_Bonus_History_Array.push({
                  BonusOwner: User_Item,
                  Amount: Final_Reward > Package_Price ? Package_Price :Final_Reward,
                  Matching: combo,
                  Rate: "8%",
                  ForwardedValue: subtractForwardValue,
                  SubtractedFrom: subtracted_From_Which_Side,
                  FlushCalculation: Flushed_Data
                });

                await PackageHistory.findOneAndUpdate(
                  { _id: Find_If_User_Have_Package[0]._id },
                  { Type2: "Basic" }
                );
                await ShortRecord.findByIdAndUpdate(
                  { _id: Find_Short_Record[0]._id },
                  { $inc: { MatcingBonus: Number(Final_Reward > Package_Price ? Package_Price :Final_Reward) } }
                );
                await User.findByIdAndUpdate(
                  { _id: Find_Short_Record[0].RecordOwner },
                  { $inc: { MainWallet: Number(Final_Reward > Package_Price ? Package_Price :Final_Reward) } }
                );


              }

            }
          }
        }
      }
    } else {
      if (Check_If_User_Already_Own_Any_Matching_Bonus.length > 0) {
        const getArrayLenght =
          Check_If_User_Already_Own_Any_Matching_Bonus.length;

        var SelectSide =
          Check_If_User_Already_Own_Any_Matching_Bonus[
            getArrayLenght - 1 < 0 ? 0 : getArrayLenght - 1
          ].SubtractedFrom;
        var subLastValue = Number(
          Check_If_User_Already_Own_Any_Matching_Bonus[
            getArrayLenght - 1 < 0 ? 0 : getArrayLenght - 1
          ].ForwardedValue
        );
      } else {
        var SelectSide = "Left";
        var subLastValue = 0;
      }

      let Package_Price = Find_If_User_Have_Package[0].PackagePrice;

      const Find_User_Directs = Users.filter(
        (e) => e.UpperlineUser == User_Item._id.toString()
      );

      if (Find_User_Directs.length !== 0) {
        var LeftWall = 0;
        var LeftWallId = "";
        var RightWall = 0;
        var RightWallId = "";

        for (let index = 0; index < Find_User_Directs.length; index++) {
          const Direct_User_Element = Find_User_Directs[index].Position;

          if (Direct_User_Element == "Right") {
            LeftWall =
              LeftWall + Number(Find_User_Directs[index].PurchasedPackagePrice);
            LeftWallId = Find_User_Directs[index]._id;
          }
          if (Direct_User_Element == "Left") {
            RightWall =
              RightWall +
              Number(Find_User_Directs[index].PurchasedPackagePrice);
            RightWallId = Find_User_Directs[index]._id;
          }
        }


        if (
          LeftWall >= Number(Package_Price) &&
          RightWall >= Number(Package_Price)
        ) {
          const currentUserBussiness = await findTotalBussiness(
            User_Item,
            totalBussinessCache
          );

          let leftBusiness = currentUserBussiness.data.leftIncome;
          let rightBusiness = currentUserBussiness.data.rightIncome;








          if (SelectSide == "Left") {
            leftBusiness = Number(leftBusiness) + Number(subLastValue);
          } else {
            rightBusiness = Number(rightBusiness) + Number(subLastValue);
          }


          console.log("----------------------------------------------------------------------")
          console.log("Left From Binary => " + leftBusiness)
          console.log("rightBusiness From Binary => " + rightBusiness)
          console.log(User_Item)
          console.log("----------------------------------------------------------------------")
          



          if (
            1 == 1
          ) {
            var combo = 0;

            if (leftBusiness < rightBusiness) {
              combo = Number(leftBusiness);

              var subtractForwardValue = rightBusiness - leftBusiness;
              var subtracted_From_Which_Side = "Right";
            } else if (rightBusiness < leftBusiness) {
              combo = Number(rightBusiness);

              var subtractForwardValue = leftBusiness - rightBusiness;
              var subtracted_From_Which_Side = "Left";
            } else if (rightBusiness == leftBusiness) {
              combo = Number(rightBusiness);
              var subtractForwardValue = 0;
              var subtracted_From_Which_Side = "Left";
            }

            var packPercantage = (Number(combo) * 8) / 100;

            const GiveMatchingBonus = Users.filter(
              (e) => e._id == User_Item.toString()
            );

            /*
                        *! GOING TO CALCULATE MAX CAPING FOR THIS USER
                        ! FORMULA ==> MAX I CAN EARN = 300
                        !             CURRENT WALLET = 280 
                        !             NEXT I WILL GET REWARD = 50 
                        !         let Value = MAX I CAN EARN  - CURRENT WALLET
                        !         let Reward = NEXT I WILL GET REWARD > Value ? Value : NEXT I WILL GET REWARD
                        */





            /*
                        ! FIND SHORT RECORD FOR THIS USER
                        */

            const Find_Short_Record = ShortRecords.filter(
              (e) => e.RecordOwner.toString() == User_Item
            );

            const FinalShort = Find_Short_Record[0]




            /*
          ! GET MY ALL EARNINGS
          */

            const All_My_Daily_Income = FinalShort ? FinalShort.DailyStakig : 0
            const All_My_Power_Income = FinalShort ? FinalShort.PowerStaing : 0
            const All_My_Direct_Reward = FinalShort ? FinalShort.DirectReward : 0
            const All_My_Matcing_Bonus = FinalShort ? FinalShort.MatcingBonus : 0
            const All_My_Rank_Eligibility = FinalShort ? FinalShort.RankEligibility : 0
            const All_My_Gobal_Pool_Bonus = FinalShort ? FinalShort.GobalPoolBonus : 0
            const All_My_Rebuy_Bonus = FinalShort ? FinalShort.RebuyBonus : 0

            const Total_Earning = All_My_Daily_Income + All_My_Power_Income + All_My_Direct_Reward + All_My_Matcing_Bonus + All_My_Rank_Eligibility + All_My_Gobal_Pool_Bonus + All_My_Rebuy_Bonus






            /*
            ! GOES TILL HERE 
            */









            const Max_I_Can_Earn = (Number(Package_Price) * 300) / 100; // Max I can earn

            const My_Current_Walet = Total_Earning

            const Future_I_Will_Get_Reward = packPercantage;

            let Reward = Max_I_Can_Earn - Users[index].MainWallet;

            let Final_Reward =
              Future_I_Will_Get_Reward > Reward
                ? Reward
                : Future_I_Will_Get_Reward;

            var I_Can_Maximum_Get = Number(Package_Price) * 300 / 100

            var Flushed_Data = 0

            if (Number(Final_Reward) > I_Can_Maximum_Get) {
              Final_Reward = I_Can_Maximum_Get
              Flushed_Data = Number(Final_Reward) - I_Can_Maximum_Get
            }

            console.log(Final_Reward)

            if (Final_Reward >= 0) {


              Matching_Bonus_History_Array.push({
                BonusOwner: User_Item,
                Amount: Final_Reward > Package_Price ? Package_Price :Final_Reward,
                Matching: combo,
                Rate: "8%",
                ForwardedValue: subtractForwardValue,
                SubtractedFrom: subtracted_From_Which_Side,
                FlushCalculation: Flushed_Data
              });
              await ShortRecord.findByIdAndUpdate(
                { _id: Find_Short_Record[0]._id },
                { $inc: { MatcingBonus: Number(Final_Reward > Package_Price ? Package_Price :Final_Reward) } }
              );
              await User.findByIdAndUpdate(
                { _id: Find_Short_Record[0].RecordOwner },
                { $inc: { MainWallet: Number(Final_Reward > Package_Price ? Package_Price :Final_Reward) } }
              );



            }

          }
        }
      }
    }
  }

  if (Matching_Bonus_History_Array.length > 0) {
    await MatchingBonusHistory.insertMany(Matching_Bonus_History_Array);
  }

  if (updateOps.length > 0) {
    await User.bulkWrite(updateOps, { ordered: false });
  }

  res.json("Matching Bonus Distributed");
};
