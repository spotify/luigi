import warnings

import nose


if __name__ == '__main__':
    with warnings.catch_warnings():
        warnings.simplefilter("default")
        warnings.filterwarnings(
            "ignore",
            message='(.*)outputs has no custom(.*)',
            category=UserWarning
        )
        nose.main()
