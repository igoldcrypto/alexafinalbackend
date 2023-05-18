const User = require("../Models/User")
const RenewalPurchasePackage = require("../Models/Renewal/RenewalPurchasePackage")
const RankEligibilityClaim = require("../Models/History/RankEligibilityClaim")
const Plan = require("../Models/Plan")
const PackageHistory = require("../Models/History/PackageHistory");
const ShortRecord = require("../Models/ShortRecord");



exports.CountMyTeam = async(req, res) => {

    const { id } = req.body;

    if (!id) {
        return res.status(500).json({message:"Please Proide User Id"})
    }

    const currentUser = await User.findById(id);

    if(!currentUser) {
   

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












    return res.json(totalData.users)



}