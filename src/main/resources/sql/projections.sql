CREATE TABLE IF NOT EXISTS RestaurantEntity
(
    id                VARCHAR(60),
    aggregate_version VARCHAR(60) NOT NULL,
    name              VARCHAR(60) NOT NULL,
    cuisine           VARCHAR(60) NOT NULL,
    menu_id           VARCHAR(60) NOT NULL,
    new_restaurant    VARCHAR(60) NOT NULL,
    PRIMARY KEY ("id")
);

CREATE TABLE IF NOT EXISTS MenuItemEntity
(
    id            VARCHAR(60),
    menu_item_id  VARCHAR(60) NOT NULL,
    restaurant_id VARCHAR(60) NOT NULL,
    name          VARCHAR(60) NOT NULL,
    price         VARCHAR(60) NOT NULL,
    menu_id       VARCHAR(60) NOT NULL,
    new_menu_item VARCHAR(60) NOT NULL,
    PRIMARY KEY ("id")
);

CREATE TABLE IF NOT EXISTS RestaurantOrderEntity
(
    id                   VARCHAR(60),
    aggregate_version    VARCHAR(60) NOT NULL,
    restaurant_id        VARCHAR(60) NOT NULL,
    state                VARCHAR(60) NOT NULL,
    new_restaurant_order VARCHAR(60) NOT NULL,
    PRIMARY KEY ("id")
);

CREATE TABLE IF NOT EXISTS RestaurantOrderItemEntity
(
    id                        VARCHAR(60),
    order_id                  VARCHAR(60) NOT NULL,
    menu_item_id              VARCHAR(60) NOT NULL,
    name                      VARCHAR(60) NOT NULL,
    quantity                  VARCHAR(60) NOT NULL,
    new_restaurant_order_item VARCHAR(60) NOT NULL,
    PRIMARY KEY ("id")
);