package clickhouse

const (
	DefaultRawBlocksTableName               = "raw_blocks"
	DefaultRawTransactionsTableName         = "raw_transactions"
	DefaultRawLogsTableName                 = "raw_logs"
	DefaultRawInternalTransactionsTableName = "internal_transactions"

	DefaultICMMessagesTableName                   = "icm_messages"
	DefaultICMSendEventsTableName                 = "icm_send_events"
	DefaultICMReceiveEventsTableName              = "icm_receive_events"
	DefaultICMMessageExecutedEventsTableName      = "icm_message_executed_events"
	DefaultICMMessageExecutionFailedEventsTableName = "icm_message_execution_failed_events"
	DefaultICMReceiptsEventsTableName             = "icm_receipts_events"
	DefaultICMFeeInfoEventsTableName              = "icm_fee_info_events"
	DefaultICMFeeRedemptionsEventsTableName       = "icm_fee_redemptions_events"
)
