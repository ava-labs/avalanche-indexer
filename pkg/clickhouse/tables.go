package clickhouse

const (
	DefaultRawBlocksTableName               = "raw_blocks"
	DefaultRawTransactionsTableName         = "raw_transactions"
	DefaultRawLogsTableName                 = "raw_logs"
	DefaultRawInternalTransactionsTableName = "internal_transactions"

	DefaultICMMessagesTableName                     = "messages"
	DefaultICMSendEventsTableName                   = "send_events"
	DefaultICMReceiveEventsTableName                = "receive_events"
	DefaultICMMessageExecutedEventsTableName        = "message_executed_events"
	DefaultICMMessageExecutionFailedEventsTableName = "message_execution_failed_events"
	DefaultICMReceiptEventsTableName                = "receipt_events"
	DefaultICMAddFeeEventsTableName                 = "add_fee_events"
	DefaultICMRelayerRewardRedeemedEventsTableName  = "relayer_reward_redeemed_events"
)
