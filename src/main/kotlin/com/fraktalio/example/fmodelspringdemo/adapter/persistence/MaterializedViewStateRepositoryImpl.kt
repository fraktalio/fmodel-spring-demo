package com.fraktalio.example.fmodelspringdemo.adapter.persistence

import com.fraktalio.example.fmodelspringdemo.adapter.deciderId
import com.fraktalio.example.fmodelspringdemo.application.MaterializedViewState
import com.fraktalio.example.fmodelspringdemo.application.MaterializedViewStateRepository
import com.fraktalio.example.fmodelspringdemo.domain.*
import kotlinx.collections.immutable.ImmutableList
import kotlinx.collections.immutable.toImmutableList
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.withContext
import kotlinx.serialization.Serializable
import mu.KotlinLogging
import org.springframework.data.annotation.Id
import org.springframework.data.domain.Persistable
import org.springframework.data.r2dbc.repository.Query
import org.springframework.data.relational.core.mapping.Column
import org.springframework.data.relational.core.mapping.Table
import org.springframework.data.repository.kotlin.CoroutineCrudRepository
import org.springframework.data.repository.query.Param
import org.springframework.transaction.reactive.TransactionalOperator
import org.springframework.transaction.reactive.executeAndAwait
import java.math.BigDecimal
import java.util.*


private val logger = KotlinLogging.logger {}

/**
 * View repository implementation
 *
 * @property restaurantRepository Restaurant repository
 * @property orderRepository Restaurant Order repository
 * @property orderItemRepository Restaurant Order Item repository
 * @property menuItemRepository Restaurant Menu Item  repository
 * @property operator Spring Reactive [TransactionalOperator]
 * @constructor Create empty Materialized View repository impl
 *
 * @author Иван Дугалић / Ivan Dugalic / @idugalic
 */

internal open class MaterializedViewStateRepositoryImpl(
    private val restaurantRepository: RestaurantCoroutineRepository,
    private val orderRepository: OrderCoroutineRepository,
    private val orderItemRepository: OrderItemCoroutineRepository,
    private val menuItemRepository: MenuItemCoroutineRepository,
    private val operator: TransactionalOperator
) : MaterializedViewStateRepository {

    @OptIn(ExperimentalCoroutinesApi::class)
    private val dbDispatcher = Dispatchers.IO.limitedParallelism(10)
    override suspend fun Event?.fetchState(): MaterializedViewState =
        withContext(dbDispatcher) {
            try {
                logger.debug { "fetching state by event ${this@fetchState} started ..." }
                val result = when (this@fetchState) {
                    is OrderEvent -> MaterializedViewState(
                        null,
                        orderRepository
                            .findById(deciderId())
                            .toOrder(orderItemRepository
                                .findByOrderId(deciderId())
                                .map { it.toOrderLineItem() }
                                .toImmutableList()
                            )
                    )

                    is RestaurantEvent -> {
                        val restaurantEntity = restaurantRepository.findById(deciderId())
                        val menuItemEntities = menuItemRepository.findByRestaurantId(deciderId())
                        MaterializedViewState(
                            restaurantEntity?.toRestaurant(
                                RestaurantMenu(
                                    menuItemEntities.map { it.toMenuItem() }.toImmutableList(),
                                    UUID.fromString(restaurantEntity.menuId),
                                    restaurantEntity.cuisine
                                )
                            ),
                            null
                        )
                    }

                    null -> MaterializedViewState(null, null)
                }
                logger.debug { "fetching state by event $this completed with success" }
                result
            } catch (e: Exception) {
                logger.error { "fetching state by event $this completed with exception $e" }
                throw e
            }
        }

    override suspend fun MaterializedViewState.save(): MaterializedViewState =
        withContext(dbDispatcher) {
            logger.debug { "saving the state started ..." }
            operator.executeAndAwait { transaction ->
                try {
                    order?.let { order ->
                        orderRepository.save(order.toOrderEntity(!orderRepository.existsById(order.id.value.toString())))
                        order.lineItems.forEach {
                            orderItemRepository.save(
                                it.toOrderItemEntity(
                                    order.id.value.toString(),
                                    !orderItemRepository.existsById(it.id.value)
                                )
                            )
                        }
                    }
                    restaurant?.let { restaurant ->
                        restaurantRepository.save(
                            restaurant.toRestaurantEntity(
                                !restaurantRepository.existsById(
                                    restaurant.id.value.toString()
                                )
                            )
                        )
                        restaurant.menu.menuItems.forEach {
                            menuItemRepository.save(
                                it.toMenuItemEntity(
                                    restaurant.menu.menuId.toString(),
                                    restaurant.id.value.toString(),
                                    !menuItemRepository.existsById(it.menuItemId.value)
                                )
                            )
                        }
                    }
                    logger.debug { "saving the state completed with success" }
                } catch (e: Exception) {
                    logger.error { "saving the state completed with exception $e" }
                    transaction.setRollbackOnly()
                    throw e
                }
            }
            this@save
        }


    private fun RestaurantEntity?.toRestaurant(menu: RestaurantMenu) = when {
        this != null -> RestaurantViewState(
            RestaurantId(UUID.fromString(id)),
            RestaurantName(name),
            menu
        )

        else -> null
    }

    private fun OrderEntity?.toOrder(lineItems: ImmutableList<OrderLineItem>): OrderViewState? =
        when {
            this != null -> OrderViewState(
                OrderId(UUID.fromString(id)),
                RestaurantId(UUID.fromString(restaurantId)),
                state,
                lineItems
            )

            else -> null
        }

    private fun OrderItemEntity.toOrderLineItem(): OrderLineItem =
        OrderLineItem(
            OrderLineItemId(id ?: ""),
            OrderLineItemQuantity(quantity),
            MenuItemId(menuItemId),
            MenuItemName(name)
        )

    private fun MenuItemEntity.toMenuItem(): MenuItem =
        MenuItem(MenuItemId(id ?: ""), MenuItemName(name), Money(this.price))


    private fun RestaurantViewState.toRestaurantEntity(isNew: Boolean) = RestaurantEntity(
        id.value.toString(),
        Long.MIN_VALUE,
        name.value,
        menu.cuisine,
        menu.menuId.toString(),
        isNew
    )

    private fun MenuItem.toMenuItemEntity(menuId: String, restaurantId: String, isNew: Boolean) = MenuItemEntity(
        menuItemId.value, menuItemId.value, menuId, restaurantId, name.value, this.price.value, isNew
    )


    private fun OrderViewState.toOrderEntity(isNew: Boolean) = OrderEntity(
        id.value.toString(),
        Long.MIN_VALUE,
        restaurantId.value.toString(),
        status,
        isNew
    )

    private fun OrderLineItem.toOrderItemEntity(orderId: String, isNew: Boolean) = OrderItemEntity(
        id.value,
        orderId,
        menuItemId.value,
        name.value,
        quantity.value,
        isNew
    )
}

internal interface RestaurantCoroutineRepository : CoroutineCrudRepository<RestaurantEntity, String>
internal interface MenuItemCoroutineRepository : CoroutineCrudRepository<MenuItemEntity, String> {
    // language=SQL
    @Query(
        """
        SELECT * FROM MenuItemEntity
        WHERE restaurant_id = :restaurantId
    """
    )
    suspend fun findByRestaurantId(@Param("restaurantId") restaurantId: String): List<MenuItemEntity>
}

@Serializable
@Table("RestaurantEntity")
internal data class RestaurantEntity(
    @Id @Column("id") var aggregateId: String? = null,
    val aggregateVersion: Long,
    val name: String,
    val cuisine: RestaurantMenuCuisine,
    val menuId: String,
    val newRestaurant: Boolean = false
) : Persistable<String> {

    override fun isNew(): Boolean = newRestaurant

    override fun getId(): String? = aggregateId
}

@Table("MenuItemEntity")
internal data class MenuItemEntity(
    @Id @Column("id") var identifier: String? = null,
    val menuItemId: String,
    val menuId: String,
    val restaurantId: String,
    val name: String,
    val price: BigDecimal,
    val newMenuItem: Boolean = false
) : Persistable<String> {

    override fun isNew(): Boolean = newMenuItem

    override fun getId(): String? = identifier
}

internal interface OrderCoroutineRepository : CoroutineCrudRepository<OrderEntity, String>
internal interface OrderItemCoroutineRepository :
    CoroutineCrudRepository<OrderItemEntity, String> {

    // language=SQL
    @Query(
        """
        SELECT * FROM RestaurantOrderItemEntity
        WHERE order_id = :order_id
    """
    )
    suspend fun findByOrderId(@Param("order_id") order_id: String): List<OrderItemEntity>
}

@Table("RestaurantOrderEntity")
internal data class OrderEntity(
    @Id @Column("id") var aggregateId: String? = null,
    val aggregateVersion: Long,
    val restaurantId: String,
    val state: OrderStatus,
    val newRestaurantOrder: Boolean = false
) : Persistable<String> {

    override fun isNew(): Boolean = newRestaurantOrder

    override fun getId(): String? = aggregateId
}

@Table("RestaurantOrderItemEntity")
internal data class OrderItemEntity(
    @Id @Column("id") var identifier: String? = null,
    val orderId: String,
    val menuItemId: String,
    val name: String,
    val quantity: Int,
    val newRestaurantOrderItem: Boolean = false
) : Persistable<String> {

    override fun isNew(): Boolean = newRestaurantOrderItem

    override fun getId(): String? = identifier
}

