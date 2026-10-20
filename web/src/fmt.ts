const time = new Intl.DateTimeFormat('en-GB', { hour: '2-digit', minute: '2-digit', second: '2-digit' })
const day = new Intl.DateTimeFormat('en-GB', { day: 'numeric', month: 'short' })
const dayTime = new Intl.DateTimeFormat('en-GB', { day: 'numeric', month: 'short', hour: '2-digit', minute: '2-digit' })

export const fmtTime = (iso: string) => time.format(new Date(iso))
export const fmtDay = (iso: string) => day.format(new Date(iso))
export const fmtDayTime = (iso: string) => dayTime.format(new Date(iso))

export const streamLabel = (connector: string, stream: string) =>
  connector === 'mattermost' && stream !== 'dm' ? `#${stream}` : stream === 'dm' ? 'DM' : stream
