import mongoose from "mongoose";

const MyTeamMember = mongoose.Schema({

    RecordOwner:{
        default: 'null',
        type: 'String'
    },
    UserId:{
        default: 'null',
        type: 'String'
    },
    Sponser:{
        default: 'null',
        type: 'String'
    },
    Position:{
        default: 'null',
        type: 'String'
    },
    Referral:{
        default: 'null',
        type: 'String'
    },
    Level:{
        default: 'null',
        type: 'String'
    }
},
{
  timestamps: true
})
export default mongoose.models.MyTeamMember || mongoose.model('MyTeamMember', MyTeamMember)
