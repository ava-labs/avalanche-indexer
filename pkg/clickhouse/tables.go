package clickhouse

const (
	DefaultRawBlocksTableName               = "raw_blocks"
	DefaultRawTransactionsTableName         = "raw_transactions"
	DefaultRawLogsTableName                 = "raw_logs"
	DefaultRawInternalTransactionsTableName = "internal_transactions"

	DefaultICMMessagesTableName                     = "icm_messages"
	DefaultICMSendEventsTableName                   = "icm_send_events"
	DefaultICMReceiveEventsTableName                = "icm_receive_events"
	DefaultICMMessageExecutedEventsTableName        = "icm_message_executed_events"
	DefaultICMMessageExecutionFailedEventsTableName = "icm_message_execution_failed_events"
	DefaultICMReceiptEventsTableName                = "icm_receipt_events"
	DefaultICMAddFeeEventsTableName                 = "icm_add_fee_events"
	DefaultICMRelayerRewardRedeemedEventsTableName  = "icm_relayer_reward_redeemed_events"
)
