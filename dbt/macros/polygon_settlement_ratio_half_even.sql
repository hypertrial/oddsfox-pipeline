{% macro polygon_settlement_ratio_half_even(numerator, denominator) -%}
case
    when
        {{ numerator }} is null
        or {{ denominator }} is null
        or {{ numerator }} < 0
        or {{ denominator }} <= 0
        or {{ numerator }}
        > cast('340282366920938.463374' as decimal(38, 6))
        or {{ denominator }}
        > cast('340282366920938.463374' as decimal(38, 6))
        then cast(null as decimal(38, 18))
    else (
        select
            cast(
                quotient_units
                + cast(
                    case
                        when
                            remainder_units
                            > denominator_units - remainder_units
                            or (
                                remainder_units
                                = denominator_units - remainder_units
                                and quotient_units
                                % cast(2 as uhugeint) = cast(1 as uhugeint)
                            )
                            then 1
                        else 0
                    end
                    as uhugeint
                )
                as decimal(38, 0)
            ) * cast('0.000000000000000001' as decimal(38, 18))
        from (
            select
                scaled_numerator_units // denominator_units as quotient_units,
                scaled_numerator_units % denominator_units as remainder_units,
                denominator_units
            from (
                select
                    try(
                        cast({{ numerator }} * 1000000 as uhugeint)
                        * cast(1000000000000000000 as uhugeint)
                    ) as scaled_numerator_units,
                    try_cast(
                        {{ denominator }} * 1000000 as uhugeint
                    ) as denominator_units
            ) as ratio_inputs
        ) as ratio_parts
    )
end
{%- endmacro %}
