-- slope with mean and std
select code,
       date,
       close,
       slope,
       slope_mean,
       slope_std
from (select code,
             date,
             close,
             coef.slope                                                                            slope,
             std(coef.slope)
                 over (partition by code order by date rows between 599 preceding and current row) slope_std,
             avg(coef.slope)
                 over (partition by code order by date rows between 599 preceding and current row) slope_mean
      from (select code,
                   date,
                   close,
                   get_ols(x, y) coef
            from (select code,
                         date,
                         close,
                         rn,
                         collect_list(close)
                                      over (partition by code order by date rows between 17 preceding and current row) y,
                         collect_list(rn)
                                      over (partition by code order by date rows between 17 preceding and current row) x
                  from (select t1.code,
                               date,
                               close,
                               row_number() over (partition by t1.code order by t1.date) rn
                        from df t1
                        ) t
                  ) t
            ) t
      ) t
order by code, date
;

-- slope without mean and std
select code,
       date,
       close,
       coef.slope
      from (select code,
                   date,
                   close,
                   get_ols(x, y) coef
            from (select code,
                         date,
                         close,
                         rn,
                         collect_list(close)
                                      over (partition by code order by date rows between 17 preceding and current row) y,
                         collect_list(rn)
                                      over (partition by code order by date rows between 17 preceding and current row) x
                  from (select t1.code,
                               date,
                               close,
                               row_number() over (partition by t1.code order by t1.date) rn
                        from df t1
                        ) t
                  ) t
            ) t
where date='{}'
order by code, date
;