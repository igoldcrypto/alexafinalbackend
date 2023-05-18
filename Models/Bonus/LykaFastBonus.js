const mongoose = require('mongoose')



const LykaFastBonus = mongoose.Schema({

    FastBonusCandidate:{
        type:"string",
        require:true
    },
    ReferLength:{
        type:"string",
        require:true
    }
},
{
  timestamps: true
})
module.exports = mongoose.model("LykaFastBonus", LykaFastBonus);
// export default mongoose.models.LykaFastBonus || mongoose.model('LykaFastBonus', LykaFastBonus)
