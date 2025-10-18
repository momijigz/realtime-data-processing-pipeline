export function getHealth() {
  return {
    status: 'ok',
    uptime: process.uptime(),
    timestamp: Date.now()
  }
}
