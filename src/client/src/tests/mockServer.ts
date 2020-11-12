import { Server, createServer } from 'net'

export function createMockServer (onData: (data: Buffer) => Buffer): Server {
  const server = createServer((connection): void => {
    connection.on('data', (data: Buffer): void => {
      const buffer = onData(data)
      connection.write(buffer)
    })
  })

  return server
}
