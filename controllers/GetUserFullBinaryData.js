const PackageHistory = require("../Models/History/PackageHistory");
const PackageInvoice = require("../Models/Invoice/PurchasePackageInvoice");
const User = require("../Models/User");
const MatchingBonusHistory = require("../Models/History/MatchingBonusHistory");
const ShortRecord = require("../Models/ShortRecord");
const moment = require("moment");
const PurchasePackageInvoice = require("../Models/Invoice/PurchasePackageInvoice");

exports.GetUserFullBinaryData = async (req, res) => {

    const {_id} = req.body;

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
    PurchasePackageInvoice.find().lean(),
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

  const currentUserBussiness = await findTotalBussiness(
    _id,
    totalBussinessCache
  );
  
  let leftBusiness = currentUserBussiness.data.leftIncome;
  let rightBusiness = currentUserBussiness.data.rightIncome;


  res.json({leftBusiness,rightBusiness});
};
