from functools import wraps

def indicator_wrapper(func):
    """
    This wrapper just stores all dataframes created during indicator build. 
    It#s mainly to avoid boilerplate code
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        class A: pass
        df_collection = A()
        try:
            func(df_collection=df_collection, *args, **kwargs)
        finally:
            config = kwargs["config"]
            for df_name in filter(lambda x: x.startswith("df_"),vars(df_collection)):
                _,folder = df_name.split("_") # df_name supuse to be somthing like df_raw
         
                path = config.output_dir / folder 
                path.mkdir(parents=False, exist_ok=True)

                getattr(df_collection, df_name).to_csv(path / f"{func.__name__}.csv")

    return wrapper