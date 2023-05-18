require("dotenv").config();
const express = require("express");
const connectToDB = require("./config/db")
const userRouter = require("./routes/userRoutes");
const app = express();
const cors = require('cors')


// globally
app.use(cors())

//middleware
app.use(express.json());
app.use(express.urlencoded({ extended: true }))


connectToDB();
console.log("came here")

app.use("/api", userRouter)


module.exports = app;
