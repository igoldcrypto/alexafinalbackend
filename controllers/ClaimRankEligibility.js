const User = require("../Models/User")
const RenewalPurchasePackage = require("../Models/Renewal/RenewalPurchasePackage")
const RankEligibilityClaim = require("../Models/History/RankEligibilityClaim")
const Plan = require("../Models/Plan")
const PackageHistory = require("../Models/History/PackageHistory");
const ShortRecord = require("../Models/ShortRecord");



exports.ClaimRankEligibility = async(req, res) => {

    const { id, ClaimedReward, TotalBusiness } = req.body;

    if (!id || !ClaimedReward || !TotalBusiness) {
        return res.status(500).json({ message: 'Please Provide All Data' })
    }

    const findRankEligibilityData = await RankEligibilityClaim.find({RankEligibilityClaimOwnerId:id})

    // Ankan Code Starts Here...
    const currentUser = await User.findById(id);

    if(!currentUser) {
        // Handle non exsistent user edge case
    }

    const currentSubUsers = await User.find({
        UpperlineUser: currentUser.id
    });



    let totalData = {
        users: 0,
        bussiness: 0,
    };

    const bfsQueue = [];

    currentSubUsers.forEach((sub) => {
        bfsQueue.push(sub);
    });


    // console.log(bfsQueue)

    while(bfsQueue.length > 0) {
        console.log("came in this queue")
        const currentUser = bfsQueue.shift();

        const latestPackageHistory = await PackageHistory.findOne({
            PackageOwner: currentUser.id
        });
        console.log(latestPackageHistory)

        if(latestPackageHistory && Number(latestPackageHistory.PackagePrice) > 0) {
            const currentUserPackage = Number(latestPackageHistory.PackagePrice);

            totalData["bussiness"] += currentUserPackage;
            totalData["users"] += 1;
        }

        const currentUsersSubs = await User.find({
            UpperlineUser: currentUser.id
        });

        currentUsersSubs.forEach((sub) => {
            bfsQueue.push(sub);
        });


    } 
    console.log(totalData.users) // The total number of this user childs





    // Ankan Code Ends Here...

    // if (findRankEligibilityData.length !== 0) {
    //     return res.status(200).json({message:"Already Given Rank Eligibility"})
    // }


    // const MainUserData = await User.findById(id)

    // const FindPackage = await Plan.findOne({ PackagePrice: MainUserData.PurchasedPackagePrice })


    // const NewWallet = Number(MainUserData.MainWallet) + Number(ClaimedReward)


    // await User.findByIdAndUpdate({ _id: id }, { MainWallet: NewWallet }) // giving reward


    // // finding renwal
    // const findOldReneal = await RenewalPurchasePackage.find({PackageOwner:id})

    // if (findOldReneal.length !== 0) {

    //     await RenewalPurchasePackage.findByIdAndUpdate({_id:findOldReneal[0]._id},{RankEligibility:"false"})
        
    // }

    // RankEligibilityClaim({

    //     RankEligibilityClaimOwnerId: MainUserData._id,
    //     RankEligibilityClaimOwnerUserName: MainUserData.SponserCode,
    //     RankEligibilityClaimOwnerEmail: MainUserData.EmailId,
    //     PackageOwnName: FindPackage.PackageName,
    //     PackageOwnPrice: FindPackage.PackagePrice,
    //     ClaimedReward: ClaimedReward,
    //     TotBusiness: TotalBusiness

    // }).save()



   return res.status(200).json(totalData.users)



}
