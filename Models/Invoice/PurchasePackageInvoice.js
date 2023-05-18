const mongoose = require('mongoose')



const PurchasePackageInvoice = new mongoose.Schema(
    {

    PackageOwner: {
        required: true,
        type: 'String'
    },
    PackageName: {
        required: true,
        type: 'String'
    },
    PackagePrice: {
        required: true,
        type: 'String'
    },
    PaackagePeriod: {
        required: true,
        type: 'String'
    },
    PackageMaximumLimit: {
        default: '500',
        type: 'String'
    },
    LykaToken: {
        required: true,
        type: 'String'
    },
    PackgeRewardWallte: {
        required: true,
        type: 'String'
    },
    Type: {
        required: true,
        type: 'String'
    }
},
    {
        timestamps: true
    })
    module.exports = mongoose.model("purchasepackageinvoices", PurchasePackageInvoice);
// export default mongoose.models.PurchasePackageInvoice || mongoose.model('PurchasePackageInvoice', PurchasePackageInvoice)
