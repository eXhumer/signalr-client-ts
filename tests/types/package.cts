import {
  HubConnection,
  HubConnectionBuilder,
  HttpClient,
  LogLevel,
  type IHttpClient,
} from '@exhumer/signalr-client'

const client: IHttpClient = new HttpClient()
const connection: HubConnection = new HubConnectionBuilder()
  .withUrl('http://localhost/hub')
  .withHttpClient(client)
  .configureLogging(LogLevel.None)
  .build()

void connection
