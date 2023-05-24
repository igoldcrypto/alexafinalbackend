const PackageHistory = require("../Models/History/PackageHistory")
const User = require("../Models/User")
const MatchingBonusHistory = require("../Models/History/MatchingBonusHistory")
const ShortRecord = require("../Models/ShortRecord")
// const EligibalPeopleForMatchingBonus = require("../Models/Bonus/MatchingBonus/EligibalPeopleForMatchingBonus")


const findTotalBussiness = async (userId, totalBussinessCache) => {
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
        let currentUser = await User.findById(userId);

        let leftUserId = currentUser.LeftTeamId;
        let rightUserId = currentUser.RightTeamId;

        const leftIncome = await findTotalBussiness(leftUserId, totalBussinessCache);
        if (!leftIncome.success) return leftIncome;

        const rightIncome = await findTotalBussiness(rightUserId, totalBussinessCache);
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


exports.MatchingBonus = async (req, res) => {


    const FindAllUsers = await User.find()

    const totalBussinessCache = {};

    for (let index = 0; index < FindAllUsers.length; index++) {

        var checkIfUserAlreadyOwnAnyMatchingBonus = await MatchingBonusHistory.findOne({ BonusOwner: FindAllUsers[index]._id })


        const FindMainUserPackage = await PackageHistory.findOne({ PackageOwner: FindAllUsers[index]._id, createdAt: { $gte: new Date((new Date().getTime() - ( 45 * 60 * 1000))) } })


        if (FindMainUserPackage && FindMainUserPackage.Type2 == "Repurchased") {


            if (FindMainUserPackage !== null) {

                if (checkIfUserAlreadyOwnAnyMatchingBonus !== null) {
                    var subLastValue = Number(checkIfUserAlreadyOwnAnyMatchingBonus.ForwardedValue)
                } else {
                    var subLastValue = 0
                }

                var PackPrice = FindMainUserPackage.PackagePrice



                const findUserDirects = await User.find({
                    UpperlineUser: FindAllUsers[index]._id,
                    createdAt: { $gte: new Date(FindMainUserPackage.createdAt) }
                })





                if (findUserDirects.length !== 0) {

                    var LeftWall = 0
                    var LeftWallId = ""
                    var RightWall = 0
                    var RightWallId = ""

                    for (let index = 0; index < findUserDirects.length; index++) {

                        if (findUserDirects[index].Position == "Right") {

                            LeftWall = LeftWall + Number(findUserDirects[index].PurchasedPackagePrice)
                            LeftWallId = findUserDirects[index]._id
                        }
                        if (findUserDirects[index].Position == "Left") {

                            RightWall = RightWall + Number(findUserDirects[index].PurchasedPackagePrice)
                            RightWallId = findUserDirects[index]._id

                        }
                    }
                }

                
                
                if (LeftWall >= Number(PackPrice) && RightWall >= Number(PackPrice)) {

                    


                        // const findThisUserData = await User.findById(FindAllUsers[index]._id)

                        const currentUserBussiness = await findTotalBussiness(FindAllUsers[index]._id, totalBussinessCache);

                        // 
    
    
    
                        let leftBusiness = currentUserBussiness.data.leftIncome
                        let rightBusiness = currentUserBussiness.data.rightIncome
    
    
    


                    var combo = 0

                    if (leftBusiness < rightBusiness) {


                        combo = Number(leftBusiness) + Number(subLastValue)
                        var subtractForwardValue = rightBusiness - leftBusiness

                    } else if (rightBusiness < leftBusiness) {


                        combo = Number(rightBusiness) + Number(subLastValue)
                        var subtractForwardValue = leftBusiness - rightBusiness

                    } else if (rightBusiness == leftBusiness) {


                        combo = Number(rightBusiness)
                        var subtractForwardValue = 0

                    }

                    var packPercantage = Number(combo) * 8 / 100

                    const GiveMatchingBonus = await User.findById(FindAllUsers[index]._id)

                    const userWallet = Number(GiveMatchingBonus.MainWallet) + Number(packPercantage)

                    const ProvideMatchingBonus = await User.findByIdAndUpdate({ _id: FindAllUsers[index]._id }, { MainWallet: userWallet })

                    const CreateRecord = await MatchingBonusHistory({
                        BonusOwner: FindAllUsers[index]._id,
                        Amount: packPercantage,
                        Matching: combo,
                        Rate: "8%",
                        ForwardedValue: subtractForwardValue
                    }).save()


                    const findShortRecord = await ShortRecord.findOne({ RecordOwner: FindAllUsers[index]._id })


                    if (findShortRecord) {

                        let sum = Number(findShortRecord.MatcingBonus) + Number(packPercantage)

                        const updateValue = await ShortRecord.findByIdAndUpdate({ _id: findShortRecord._id }, { MatcingBonus: sum })

                    } else {

                        const createShortRecord = await ShortRecord({
                            RecordOwner: FindAllUsers[index]._id,
                            MatcingBonus: packPercantage
                        }).save()

                    }


                    await PackageHistory.findOneAndUpdate({_id:FindMainUserPackage._id},{Type2:"Basic"})










                } else {

                    

                }
            }







        } else {

            
            if (FindMainUserPackage !== null) {

                if (checkIfUserAlreadyOwnAnyMatchingBonus !== null) {
                    var SelectSide = checkIfUserAlreadyOwnAnyMatchingBonus.SubtractedFrom
                    var subLastValue = Number(checkIfUserAlreadyOwnAnyMatchingBonus.ForwardedValue)
                } else {
                    var SelectSide = "Left"
                    var subLastValue = 0
                }


                var PackPrice = FindMainUserPackage.PackagePrice


                const findUserDirects = await User.find({ UpperlineUser: FindAllUsers[index]._id })

                if (findUserDirects.length !== 0) {

                    var LeftWall = 0
                    var LeftWallId = ""
                    var RightWall = 0
                    var RightWallId = ""

                    for (let index = 0; index < findUserDirects.length; index++) {

                        if (findUserDirects[index].Position == "Right") {

                            LeftWall = LeftWall + Number(findUserDirects[index].PurchasedPackagePrice)
                            LeftWallId = findUserDirects[index]._id
                        }
                        if (findUserDirects[index].Position == "Left") {

                            RightWall = RightWall + Number(findUserDirects[index].PurchasedPackagePrice)
                            RightWallId = findUserDirects[index]._id

                        }
                    }
                }
                
                
                if (LeftWall >= Number(PackPrice) && RightWall >= Number(PackPrice)) {
                    

                    const currentUserBussiness = await findTotalBussiness(FindAllUsers[index]._id, totalBussinessCache);


                    let leftBusiness = currentUserBussiness.data.leftIncome 
                    let rightBusiness = currentUserBussiness.data.rightIncome


                    console.log("subLastValue => "+subLastValue)

                    if (SelectSide == "Left") {
                        leftBusiness = Number(leftBusiness) + Number(subLastValue)

                    }else{
                        rightBusiness = Number(rightBusiness) + Number(subLastValue)

                    }








                    if (leftBusiness >= Number(PackPrice) && rightBusiness >= Number(PackPrice)) {


                        var combo = 0

                        if (leftBusiness < rightBusiness) {     
                          
                                    
                            combo = Number(leftBusiness)
                            var subtractForwardValue = rightBusiness - leftBusiness

                            var subtracted_From_Which_Side = "Right"

                            
                            
                            



                        } else if (rightBusiness < leftBusiness) {

                            
                            combo = Number(rightBusiness) 
                            
                            // combo = Number(rightBusiness) + Number(subLastValue)

                            // if (SelectSide == "Left") {
                                
                            //     combo = Number(leftBusiness) + Number(subLastValue)
                            //     subLastValue = 0
                                
                            // }else{
                                
                            //     subLastValue = 0

                            // }
                            var subtractForwardValue = leftBusiness - rightBusiness
                            var subtracted_From_Which_Side = "Left"
                            
                        } else if (rightBusiness == leftBusiness) {
                            
                            

                            combo = Number(rightBusiness)
                            var subtractForwardValue = 0
                            var subtracted_From_Which_Side = "Left"


                        }

                        var packPercantage = Number(combo) * 8 / 100

                        const GiveMatchingBonus = await User.findById(FindAllUsers[index]._id)

                        const userWallet = Number(GiveMatchingBonus.MainWallet) + Number(packPercantage)

                        const ProvideMatchingBonus = await User.findByIdAndUpdate({ _id: FindAllUsers[index]._id }, { MainWallet: userWallet })

                        const CreateRecord = await MatchingBonusHistory({
                            BonusOwner: FindAllUsers[index]._id,
                            Amount: packPercantage,
                            Matching: combo,
                            Rate: "8%",
                            ForwardedValue: subtractForwardValue,
                            SubtractedFrom: subtracted_From_Which_Side
                        }).save()



                        /*
                        ! SHORT RECORD WORK GOING ON BELOW
                        */

                        const findShortRecord = await ShortRecord.findOne({ RecordOwner: FindAllUsers[index]._id })


                        if (findShortRecord) {

                            let sum = Number(findShortRecord.MatcingBonus) + Number(packPercantage)

                            const updateValue = await ShortRecord.findByIdAndUpdate({ _id: findShortRecord._id }, { MatcingBonus: sum })

                        } else {

                            const createShortRecord = await ShortRecord({
                                RecordOwner: FindAllUsers[index]._id,
                                MatcingBonus: packPercantage
                            }).save()

                        }










                    }














                } else {


                }
            }

        }

    }

    res.json("done")









}
