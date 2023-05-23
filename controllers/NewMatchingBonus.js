const PackageHistory = require("../Models/History/PackageHistory")
const User = require("../Models/User")
const MatchingBonusHistory = require("../Models/History/MatchingBonusHistory")
const ShortRecord = require("../Models/ShortRecord")
const moment = require('moment');

exports.NewMatchingBonus = async (req, res) => {

    var updateOps = []

    const Matching_Bonus_History_Array = [];

    let totalBussinessCache = {}

    const [PackageHistorys, Users, MatchingBonusHistorys, ShortRecords] = await Promise.all([
        PackageHistory.find().lean(),
        User.find().lean(),
        MatchingBonusHistory.find().lean(),
        ShortRecord.find().lean()
    ])

    const findTotalBussiness = (userId, totalBussinessCache) => {
        if (userId == "null") {
            return {
                success: true,
                data: {
                    leftIncome: 0,
                    rightIncome: 0,
                    totalIncome: 0,
                },
            };
        }

        if (totalBussinessCache[userId] !== undefined) return {
            success: true,
            data: totalBussinessCache[userId]
        };


        try {

            let currentUser = Users.find((e) => e._id.toString() == userId.toString())


            let leftUserId = currentUser.LeftTeamId;
            let rightUserId = currentUser.RightTeamId;

            const leftIncome = findTotalBussiness(leftUserId, totalBussinessCache);
            if (!leftIncome.success) return leftIncome;

            const rightIncome = findTotalBussiness(rightUserId, totalBussinessCache);
            if (!rightIncome.success) return rightIncome;

            const returningIncome = {
                leftIncome: leftIncome.data.totalIncome,
                rightIncome: rightIncome.data.totalIncome,
                totalIncome: leftIncome.data.totalIncome + rightIncome.data.totalIncome + currentUser.PurchasedPackagePrice,
            };

            totalBussinessCache[userId] = returningIncome;

            return {
                success: true,
                data: returningIncome
            };
        }
        catch (error) {
            if (error instanceof Error || error instanceof MongoServerError) {
                return {
                    success: false,
                    error: error.message,
                };
            }

            return {
                success: false,
                error: "Internal Server Error"
            };
        }
    }

    for (let index = 0; index < Users.length; index++) {

        const User_Item = Users[index]._id;

        const Check_If_User_Already_Own_Any_Matching_Bonus = MatchingBonusHistorys.filter((e) => e.BonusOwner == User_Item.toString())

        const Find_If_User_Have_Package = PackageHistorys.filter((e) => e.PackageOwner == User_Item.toString())

        if (Find_If_User_Have_Package.length == 0) continue;

        if (Find_If_User_Have_Package.length > 0 && Find_If_User_Have_Package[0].Type2 == "Repurchased") {


            if (Check_If_User_Already_Own_Any_Matching_Bonus.length > 0) {
                // var SelectSide = Check_If_User_Already_Own_Any_Matching_Bonus[0].SubtractedFrom
                // var subLastValue = Number(Check_If_User_Already_Own_Any_Matching_Bonus[0].ForwardedValue)

                const getArrayLenght = Check_If_User_Already_Own_Any_Matching_Bonus.length

                var SelectSide = Check_If_User_Already_Own_Any_Matching_Bonus[getArrayLenght - 1 < 0 ? 0 : getArrayLenght - 1].SubtractedFrom
                var subLastValue = Number(Check_If_User_Already_Own_Any_Matching_Bonus[getArrayLenght - 1 < 0 ? 0 : getArrayLenght - 1].ForwardedValue)


            } else {
                var SelectSide = "Left"
                var subLastValue = 0
            }

            let Package_Price = Find_If_User_Have_Package[0].PackagePrice

            const Find_User_Directs = await User.find({
                UpperlineUser: User_Item._id,
                createdAt: { $gte: new Date(Find_If_User_Have_Package[0].createdAt) }
            })



            if (Find_User_Directs.length !== 0) {

                var LeftWall = 0
                var LeftWallId = ""
                var RightWall = 0
                var RightWallId = ""

                for (let index = 0; index < Find_User_Directs.length; index++) {

                    const Direct_User_Element = Find_User_Directs[index].Position;

                    if (Direct_User_Element == "Right") {
                        LeftWall = LeftWall + Number(Find_User_Directs[index].PurchasedPackagePrice)
                        LeftWallId = Find_User_Directs[index]._id
                    }
                    if (Direct_User_Element == "Left") {
                        RightWall = RightWall + Number(Find_User_Directs[index].PurchasedPackagePrice)
                        RightWallId = Find_User_Directs[index]._id
                    }
                }


                if (LeftWall >= Number(Package_Price) && RightWall >= Number(Package_Price)) {

                    const currentUserBussiness = findTotalBussiness(User_Item, totalBussinessCache);


                    let leftBusiness = currentUserBussiness.data.leftIncome
                    let rightBusiness = currentUserBussiness.data.rightIncome


                    if (SelectSide == "Left") {





                        leftBusiness = Number(leftBusiness) + Number(subLastValue)
                    } else {
                        rightBusiness = Number(rightBusiness) + Number(subLastValue)
                    }




                    if (leftBusiness >= Number(Package_Price) && rightBusiness >= Number(Package_Price)) {

                        var combo = 0


                        if (leftBusiness < rightBusiness) {

                            combo = Number(leftBusiness)
                            var subtractForwardValue = rightBusiness - leftBusiness
                            var subtracted_From_Which_Side = "Right"

                        } else if (rightBusiness < leftBusiness) {

                            combo = Number(rightBusiness)
                            var subtractForwardValue = leftBusiness - rightBusiness
                            var subtracted_From_Which_Side = "Left"

                        } else if (rightBusiness == leftBusiness) {

                            combo = Number(rightBusiness)
                            var subtractForwardValue = 0
                            var subtracted_From_Which_Side = "Left"

                        }


                        /*
                        ! FIND SHORT RECORD FOR THIS USER
                        */

                        const Find_Short_Record = ShortRecords.filter((e) => e.RecordOwner.toString() == User_Item)


                        var packPercantage = Number(combo) * 8 / 100

                        const GiveMatchingBonus = Users.filter((e) => e._id == User_Item.toString())
                        // const GiveMatchingBonus = Users.find({_id:User_Item})

                        // const userWallet = Number(GiveMatchingBonus[0].MainWallet) + Number(packPercantage)

                        // var updateOps = Users.map(({ _id, MainWallet }) => ({
                        //     updateOne: {
                        //         filter: { _id: _id },
                        //         update: { $set: { MainWallet: userWallet } }
                        //     }
                        // }));


                        /*
                        *! GOING TO CALCULATE MAX CAPING FOR THIS USER
                        ! FORMULA ==> MAX I CAN EARN = 300
                        !             CURRENT WALLET = 280 
                        !             NEXT I WILL GET REWARD = 50 
                        !         let Value = MAX I CAN EARN  - CURRENT WALLET
                        !         let Reward = NEXT I WILL GET REWARD > Value ? Value : NEXT I WILL GET REWARD
                        */

                        const Max_I_Can_Earn = Number(Package_Price) * 300 / 100  // Max I can earn

                        const My_Current_Walet = Number(GiveMatchingBonus[0].MainWallet)

                        const Future_I_Will_Get_Reward = packPercantage

                        let Reward = Max_I_Can_Earn - My_Current_Walet

                        let Final_Reward = Future_I_Will_Get_Reward > Reward ? Reward : Future_I_Will_Get_Reward

                        const userWallet = Number(GiveMatchingBonus[0].MainWallet) + Number(Final_Reward)


                        var updateOps = Users.map(({ _id, MainWallet }) => ({
                            updateOne: {
                                filter: { _id: _id },
                                update: { $set: { MainWallet: userWallet } }
                            }
                        }));

                        // MAX CAPING DONE

                        Matching_Bonus_History_Array.push({
                            BonusOwner: User_Item,
                            Amount: Final_Reward,
                            Matching: combo,
                            Rate: "8%",
                            ForwardedValue: subtractForwardValue,
                            SubtractedFrom: subtracted_From_Which_Side
                        })

                        await PackageHistory.findOneAndUpdate({ _id: Find_If_User_Have_Package[0]._id }, { Type2: "Basic" })
                        await ShortRecord.findByIdAndUpdate({ _id: Find_Short_Record[0]._id }, { $inc: { MatcingBonus: Number(Final_Reward) } })

                    }
                }
            }

        } else {

            if (Check_If_User_Already_Own_Any_Matching_Bonus.length > 0) {

                const getArrayLenght = Check_If_User_Already_Own_Any_Matching_Bonus.length

                var SelectSide = Check_If_User_Already_Own_Any_Matching_Bonus[getArrayLenght - 1 < 0 ? 0 : getArrayLenght - 1].SubtractedFrom
                var subLastValue = Number(Check_If_User_Already_Own_Any_Matching_Bonus[getArrayLenght - 1 < 0 ? 0 : getArrayLenght - 1].ForwardedValue)


            } else {
                var SelectSide = "Left"
                var subLastValue = 0
            }

            let Package_Price = Find_If_User_Have_Package[0].PackagePrice

            // const Find_User_Directs = Users.filter((e) => e.UpperlineUser == User_Item._id.toString())

            const Find_User_Directs = Users.filter((e) => {

                const fiveMinutesAgo = moment().subtract(60, 'minutes');
                const elementDate = moment(e.createdAt);

                return elementDate.isAfter(fiveMinutesAgo);
            }).filter((e) => e.UpperlineUser === User_Item._id.toString());

            if (Find_User_Directs.length !== 0) {

                var LeftWall = 0
                var LeftWallId = ""
                var RightWall = 0
                var RightWallId = ""

                for (let index = 0; index < Find_User_Directs.length; index++) {

                    const Direct_User_Element = Find_User_Directs[index].Position;

                    if (Direct_User_Element == "Right") {
                        LeftWall = LeftWall + Number(Find_User_Directs[index].PurchasedPackagePrice)
                        LeftWallId = Find_User_Directs[index]._id
                    }
                    if (Direct_User_Element == "Left") {
                        RightWall = RightWall + Number(Find_User_Directs[index].PurchasedPackagePrice)
                        RightWallId = Find_User_Directs[index]._id
                    }
                }


                if (LeftWall >= Number(Package_Price) && RightWall >= Number(Package_Price)) {

                    const currentUserBussiness = findTotalBussiness(User_Item, totalBussinessCache);


                    let leftBusiness = currentUserBussiness.data.leftIncome
                    let rightBusiness = currentUserBussiness.data.rightIncome


                    if (SelectSide == "Left") {

                        leftBusiness = Number(leftBusiness) + Number(subLastValue)


                    } else {
                        rightBusiness = Number(rightBusiness) + Number(subLastValue)

                    }

                    if (leftBusiness >= Number(Package_Price) && rightBusiness >= Number(Package_Price)) {

                        var combo = 0


                        if (leftBusiness < rightBusiness) {

                            combo = Number(leftBusiness)

                            var subtractForwardValue = rightBusiness - leftBusiness
                            var subtracted_From_Which_Side = "Right"


                        } else if (rightBusiness < leftBusiness) {



                            var subtractForwardValue = leftBusiness - rightBusiness
                            var subtracted_From_Which_Side = "Left"

                        } else if (rightBusiness == leftBusiness) {

                            combo = Number(rightBusiness)
                            var subtractForwardValue = 0
                            var subtracted_From_Which_Side = "Left"

                        }

                        var packPercantage = Number(combo) * 8 / 100

                        const GiveMatchingBonus = Users.filter((e) => e._id == User_Item.toString())


                        /*
                        *! GOING TO CALCULATE MAX CAPING FOR THIS USER
                        ! FORMULA ==> MAX I CAN EARN = 300
                        !             CURRENT WALLET = 280 
                        !             NEXT I WILL GET REWARD = 50 
                        !         let Value = MAX I CAN EARN  - CURRENT WALLET
                        !         let Reward = NEXT I WILL GET REWARD > Value ? Value : NEXT I WILL GET REWARD
                        */

                        const Max_I_Can_Earn = Number(Package_Price) * 300 / 100  // Max I can earn

                        const My_Current_Walet = Number(GiveMatchingBonus[0].MainWallet)

                        const Future_I_Will_Get_Reward = packPercantage

                        let Reward = Max_I_Can_Earn - My_Current_Walet

                        let Final_Reward = Future_I_Will_Get_Reward > Reward ? Reward : Future_I_Will_Get_Reward

                        const userWallet = Number(GiveMatchingBonus[0].MainWallet) + Number(Final_Reward)


                        updateOps = Users.map(({ _id, MainWallet }) => ({
                            updateOne: {
                                filter: { _id: _id },
                                update: { $set: { MainWallet: userWallet } }
                            }
                        }));

                        // MAX CAPING DONE


                        /*
                        ! FIND SHORT RECORD FOR THIS USER
                        */

                        const Find_Short_Record = ShortRecords.filter((e) => e.RecordOwner.toString() == User_Item)

                        Matching_Bonus_History_Array.push({
                            BonusOwner: User_Item,
                            Amount: Final_Reward,
                            Matching: combo,
                            Rate: "8%",
                            ForwardedValue: subtractForwardValue,
                            SubtractedFrom: subtracted_From_Which_Side
                        })
                        await ShortRecord.findByIdAndUpdate({ _id: Find_Short_Record[0]._id }, { $inc: { MatcingBonus: Number(Final_Reward) } })

                    }
                }
            }
        }

    }

    

    if (Matching_Bonus_History_Array.length > 0) {
        await MatchingBonusHistory.insertMany(Matching_Bonus_History_Array)
    }

    

    if (updateOps.length > 0) {
        await User.bulkWrite(updateOps, { ordered: false })
    }

    res.json("Matching Bonus Distributed")

}
