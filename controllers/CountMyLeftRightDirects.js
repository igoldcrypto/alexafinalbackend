const User = require("../Models/User")
const PackageHistory = require("../Models/History/PackageHistory")

exports.CountMyLeftRightDirects = async (req, res) => {

    console.log("came in api")

    const { id } = req.body;

    let MyLeftDirectBusiness = 0
    let MyRightDirectBusiness = 0

    if (!id) return res.status(500).json("Please Pass id In Body")

    let FromDate = new Date(2023, 5, 1)
    let ToDate = new Date(2023, 5, 1)

    const [users, PackageHistorys] = await Promise.all([
        User.find().select("_id UpperlineUser Position").lean(),
        PackageHistory.find(
            {
                created_on: {
                    $gte: FromDate,
                    $lt: ToDate
                }
            }
        ).select("_id PackageOwner PackagePrice").lean()
    ])


    users.filter((e) => e.UpperlineUser === id).map((hit) => {

        if (hit.Position == "Right") {

            PackageHistorys.filter((e) => e.PackageOwner === (hit._id).toString()).map((hit) => {

                return MyLeftDirectBusiness = MyLeftDirectBusiness + Number(hit.PackagePrice)

            })


        } else if (hit.Position == "Left") {

            PackageHistorys.filter((e) => e.PackageOwner === (hit._id).toString()).map((hit) => {

                return MyRightDirectBusiness = MyRightDirectBusiness + Number(hit.PackagePrice)

            })

        }

    })



    return res.json({ MyLeftDirectBusiness, MyRightDirectBusiness })

}