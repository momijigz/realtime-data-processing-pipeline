import { Router } from 'express'
import { getHealth } from '../services/healthService'

const router = Router()

router.get('/', (req, res) => {
  const status = getHealth()
  res.json(status)
})

export default router
