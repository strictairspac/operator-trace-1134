/** @typedef {{id:number,name:string,sensors:object[],readings:object[]}} Dataset */
/** @typedef {{id:number,name:string,dataset_id:number,steps:object[],note:string}} Plan */
/** @typedef {{id:number,name:string,status:'queued'|'running'|'completed'|'failed',result:object|null,snapshot_id:number|null,snapshot_refs?:number,snapshot_bytes?:number,snapshot_usage?:{dataset_id:number,used_bytes:number,limit_bytes:number}}} Trace */
export {};
