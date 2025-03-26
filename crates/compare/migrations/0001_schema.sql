create table if not exists user_transactions (
    trans_time timestamp not null,
    user_addr character[16] not null,
    trans_hash character[16] not null
);

create index transactions_index on user_transactions
(user_addr, trans_time) include (trans_hash);
