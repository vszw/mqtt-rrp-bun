import { EventEmitter } from 'events'
import MQTT, { type IClientOptions } from 'mqtt'
import { TypedEventEmitter } from 'mqtt/lib/TypedEmitter'

type TPayload = {
    ts: number
    data: any
    from_topic: string
    request_id: string | null
    callback_id: string | null
}

type TRequestResponseEvents = {
    'message': (payload: TPayload) => void
}

class MQTTRequestResponseProtocol extends TypedEventEmitter<TRequestResponseEvents> {
    private Client!: MQTT.MqttClient
    private Identifier: string
    private EE: EventEmitter = new EventEmitter()
    private Topics: string[] = []
    private Initialized: boolean = false
    private RequestTimeout: number = 3_000

    constructor(identifier: string, topics: string[]) {
        super()
        this.Identifier = identifier
        this.EE.setMaxListeners(100)
        this.Topics = topics
    }

    async init(options: IClientOptions) {
        if ( !this.Initialized ) {
            this.Client = await MQTT.connectAsync(options)
            await this.Client.subscribeAsync([ this.Identifier, ...this.Topics ])

            this.Initialized = true
            this.handleEvents()
        }
    }

    set requestTimeout(value: number) {
        this.RequestTimeout = value
    }

    private handleEvents() {
        this.Client.on('message', this.handleMessageEvents)
    }

    private handleMessageEvents(topic: string, message: Buffer) {
        let payload: TPayload | null = null
        
        try {
            payload = JSON.parse(message.toString()) as TPayload
        } catch (e) {
            throw e
        }

        if ( payload.callback_id && topic === this.Identifier ) {
            this.EE.emit(payload.callback_id, payload)
            this.EE.removeAllListeners(payload.callback_id)
            return
        }

        this.emit('message', payload)
    }

    private serializeData(data: any, request_id: string | null = null, callback_id: string | null = null) {
        return JSON.stringify({
            ts: Date.now(),
            data: data,
            from_topic: this.Identifier,
            request_id: request_id,
            callback_id: callback_id
        })
    }

    async send(topic: string, data: any, callback_id: string | null = null) {
        await this.Client.publishAsync(topic, this.serializeData(data, null, callback_id))
    }

    async request(topic: string, data: any, returnFull: boolean = false) {
        return new Promise((resolve, reject) => {
            const requestId = crypto.randomUUID()

            const timeout = setTimeout(() => {
                this.EE.removeAllListeners(requestId)
                reject(new Error(`Request ${topic} timed out.`))
            }, this.RequestTimeout)

            this.EE.once(requestId, (payload: TPayload) => {
                if ( payload.callback_id === requestId ) {
                    this.EE.removeAllListeners(requestId)
                    clearTimeout(timeout)
                    resolve(returnFull ? payload : payload.data)
                }
            })

            this.Client.publish(topic, this.serializeData(data, requestId))
        })
    }
}

export default MQTTRequestResponseProtocol
export { type TRequestResponseEvents }