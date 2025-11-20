import EventEmitter from 'events'

export type ChannelEmitter = EventEmitter

export type Channels = {
  new_plc_event: ChannelEmitter
  outgoing_plc_seq: ChannelEmitter
}

export type ChannelEvt = keyof Channels

export function createChannels(): Channels {
  return {
    new_plc_event: new EventEmitter(),
    outgoing_plc_seq: new EventEmitter(),
  }
}
