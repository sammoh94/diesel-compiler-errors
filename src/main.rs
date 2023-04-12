mod schema;

#[macro_use]
extern crate diesel;

use crate::diesel::QueryDsl;
use chrono::prelude::*;
use diesel::{
    expression_methods::ExpressionMethods, pg::{Pg, sql_types}, BoolExpressionMethods, Connection, JoinOnDsl, RunQueryDsl,
    QueryResult, BoxableExpression, dsl::{And, Eq, IsNull, IntoBoxed, LeftJoinOn, InnerJoinOn},
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use schema::*;

fn main() {
    println!("Hello, world!");
}

#[derive(Clone, Debug, Queryable, Serialize, Deserialize)]
#[diesel(table_name = p_dables)]
#[serde(rename_all = "camelCase")]
pub struct PDable {
    pub id: Uuid,
    pub p_id: Uuid,
    pub dable_id: Uuid,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub deleted_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, Queryable, Serialize, Deserialize)]
#[diesel(table_name = dables)]
#[serde(rename_all = "camelCase")]
pub struct Dable {
    pub id: Uuid,
    pub name: String,
    pub description: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub deleted_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, Queryable, Serialize, Deserialize)]
#[diesel(table_name = dable_m_m)]
#[serde(rename_all = "camelCase")]
pub struct DableMM {
    pub id: Uuid,
    pub dable_id: Uuid,
    pub name: String,
    pub description: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub deleted_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, Queryable, Serialize, Deserialize)]
#[diesel(table_name = dable_l_m)]
#[serde(rename_all = "camelCase")]
pub struct DableLM {
    pub id: Uuid,
    pub dable_m_m_id: Uuid,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub deleted_at: Option<DateTime<Utc>>,
}

pub type JoinedType = (PDable, Dable, Option<(DableMM, Option<DableLM>)>);

type PDableAlias = 
    LeftJoinOn<
        InnerJoinOn<
            schema::p_dables::table,
            schema::dables::table,
            And<
                Eq<
                    schema::p_dables::columns::dable_id,
                    schema::dables::columns::id
                >,
                IsNull<
                    schema::dables::columns::deleted_at
                >
            >
        >,
        LeftJoinOn<
            schema::dable_m_m::table,
            schema::dable_l_m::table,
            And<
                Eq<
                    schema::dable_m_m::columns::id,
                    schema::dable_l_m::columns::dable_m_m_id
                >,
                IsNull<
                    schema::dable_l_m::columns::deleted_at
                >
            >
        >,
        And<
            Eq<
                schema::dable_m_m::columns::dable_id,
                schema::dables::columns::id
            >,
            IsNull<
            schema::dable_m_m::columns::deleted_at>
        >
    >;

type BoxedQuery<'a> = IntoBoxed<'a, PDableAlias, diesel::pg::Pg>;

pub fn get_boxed_query<C: Connection<Backend = Pg> + diesel::connection::LoadConnection>(conn: &mut C) -> BoxedQuery {
    let q =
        schema::p_dables::dsl::p_dables
            .order(schema::p_dables::dsl::created_at)
            .filter(schema::p_dables::dsl::deleted_at.is_null())
            .inner_join(
                dables::dsl::dables.on(schema::p_dables::dsl::dable_id
                    .eq(dables::dsl::id)
                    .and(schema::dables::dsl::deleted_at.is_null())),
            )
            .left_join(
                schema::dable_m_m::dsl::dable_m_m
                    .left_join(schema::dable_l_m::dsl::dable_l_m.on(
                        schema::dable_m_m::dsl::id
                            .eq(schema::dable_l_m::dsl::dable_m_m_id)
                            .and(schema::dable_l_m::dsl::deleted_at.is_null())
                        )
                    )
                    .on(
                        schema::dable_m_m::dsl::dable_id
                        .eq(schema::dables::dsl::id)
                        .and(schema::dable_m_m::dsl::deleted_at.is_null())   
                    )
            )
            .into_boxed();

    q
}

pub fn get_joined<C: Connection<Backend = Pg> + diesel::connection::LoadConnection>(
    connection: &mut C,
) -> QueryResult<Vec<JoinedType>> {
    let mut q = schema::p_dables::dsl::p_dables
            .order(schema::p_dables::dsl::created_at)
            .filter(schema::p_dables::dsl::deleted_at.is_null())
            .inner_join(
                dables::dsl::dables.on(schema::p_dables::dsl::dable_id
                    .eq(dables::dsl::id)
                    .and(schema::dables::dsl::deleted_at.is_null())),
            )
            .left_join(
                schema::dable_m_m::dsl::dable_m_m
                    .left_join(schema::dable_l_m::dsl::dable_l_m.on(
                        schema::dable_m_m::dsl::id
                            .eq(schema::dable_l_m::dsl::dable_m_m_id)
                            .and(schema::dable_l_m::dsl::deleted_at.is_null())
                        )
                    )
                    .on(
                        schema::dable_m_m::dsl::dable_id
                        .eq(schema::dables::dsl::id)
                        .and(schema::dable_m_m::dsl::deleted_at.is_null())   
                    )
            ).into_boxed();


    // let order_by = order_by_expr();
    // q = q.then_order_by(Box::new(schema::p_dables::dsl::created_at.asc()));
    // ISSUE 1: This line leads to a compiler error however the line above does not
    q = q.then_order_by(order_by);
    let id = Uuid::new_v4();
    // let filter_by_expr = filter_by_expr(id);
    // q = q.filter(Box::new(schema::p_dables::dsl::id.eq(id)));
    // ISSUE 2: Same issue here. This leads to a compiler error but the line above does not
    q = q.filter(filter_by_expr);

    q.get_results(connection)
}

pub fn get_boxed_join<C: Connection<Backend = Pg> + diesel::connection::LoadConnection>(
    connection: &mut C
) -> QueryResult<()> {
    // Question 3: It seems like if we use a boxed query inside the group_by, that leads to a compiler error
    // However when we have the query defined directly and are not boxing it, there are no issues
    // Does diesel allow boxed queries inside group_by's?
    let q = get_boxed_query(connection);
    // let q = schema::p_dables::dsl::p_dables
    //         .order(schema::p_dables::dsl::created_at)
    //         .filter(schema::p_dables::dsl::deleted_at.is_null())
    //         .inner_join(
    //             dables::dsl::dables.on(schema::p_dables::dsl::dable_id
    //                 .eq(dables::dsl::id)
    //                 .and(schema::dables::dsl::deleted_at.is_null())),
    //         )
    //         .left_join(
    //             schema::dable_m_m::dsl::dable_m_m
    //                 .left_join(schema::dable_l_m::dsl::dable_l_m.on(
    //                     schema::dable_m_m::dsl::id
    //                         .eq(schema::dable_l_m::dsl::dable_m_m_id)
    //                         .and(schema::dable_l_m::dsl::deleted_at.is_null())
    //                     )
    //                 )
    //                 .on(
    //                     schema::dable_m_m::dsl::dable_id
    //                     .eq(schema::dables::dsl::id)
    //                     .and(schema::dable_m_m::dsl::deleted_at.is_null())   
    //                 )
    //         );

    let _: Vec<(Uuid, DateTime<Utc>)> = q
        .group_by(crate::schema::p_dables::dsl::id)
        .select((
            crate::schema::p_dables::dsl::id,
            diesel::dsl::sql::<sql_types::Timestamptz>("max(p_dables.created_at)")
        ))
        .get_results(connection)?;

    Ok(())

}

type BoxablePDableExpression<T> = Box<dyn BoxableExpression<PDableAlias, diesel::pg::Pg, SqlType = T>>;

fn order_by_expr() -> BoxablePDableExpression<diesel::expression::expression_types::NotSelectable> {
    Box::new(schema::p_dables::dsl::created_at.asc())
}

fn filter_by_expr(id: Uuid) -> BoxablePDableExpression<diesel::sql_types::Bool> {
    Box::new(schema::p_dables::dsl::id.eq(id))
}