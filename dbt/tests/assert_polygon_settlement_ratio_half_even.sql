{{ config(tags=['polygon_settlement']) }}

with ratio_cases (numerator, denominator, expected_ratio) as (
    values
    (
        cast('7.000000' as decimal(38, 6)),
        cast('100.000000' as decimal(38, 6)),
        cast('0.070000000000000000' as decimal(38, 18))
    ),
    (
        cast('1.000000' as decimal(38, 6)),
        cast('3.000000' as decimal(38, 6)),
        cast('0.333333333333333333' as decimal(38, 18))
    ),
    (
        cast('0.000001' as decimal(38, 6)),
        cast('2000000000000.000000' as decimal(38, 6)),
        cast('0.000000000000000000' as decimal(38, 18))
    ),
    (
        cast('0.000003' as decimal(38, 6)),
        cast('2000000000000.000000' as decimal(38, 6)),
        cast('0.000000000000000002' as decimal(38, 18))
    ),
    (
        cast('170141183460469.231687' as decimal(38, 6)),
        cast('340282366920938.463374' as decimal(38, 6)),
        cast('0.500000000000000000' as decimal(38, 18))
    )
)

select *
from ratio_cases
where
    {{ polygon_settlement_ratio_half_even('numerator', 'denominator') }}
    <> expected_ratio
