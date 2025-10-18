import express from 'express'
import helmet from 'helmet'
import cors from 'cors'
import morgan from 'morgan'
import healthRouter from './routes/health'

const app = express()

app.use(helmet())
app.use(cors())
app.use(express.json())
app.use(morgan('dev'))

app.use('/health', healthRouter)

app.get('/', (req, res) => {
  res.json({status: 'ok', name: 'realtime-data-processing-pipeline-server'})
})

export default app
