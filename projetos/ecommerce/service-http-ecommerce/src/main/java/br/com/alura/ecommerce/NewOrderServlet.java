package br.com.alura.ecommerce;

import br.com.alura.ecommerce.database.LocalDatabase;
import br.com.alura.ecommerce.dispatcher.KafkaDispatcher;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderServlet extends HttpServlet {

    private final KafkaDispatcher<Order> orderDispatcher = new KafkaDispatcher<>();

    @Override
    public void destroy() {
        super.destroy();
        orderDispatcher.close();
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        try  {
            //Apenas para teste
            var email =req.getParameter("email");
            var amount = new BigDecimal(req.getParameter("amount"));
            var orderId = req.getParameter("uuid");
            var order = new Order(orderId, amount, email);

            try(var database = new OrdersDataBase()) {
                if (database.saveNew(order)) {
                    orderDispatcher.send("ECOMMERCE_NEW_ORDER", email,
                            new CorrelationId(NewOrderServlet.class.getSimpleName()),
                            order);
                    System.out.println("New order sent successfully.");
                    resp.getWriter().println("New order sent");
                    resp.setStatus(HttpServletResponse.SC_OK);
                } else {
                    System.out.println("Order already exists.");
                    resp.getWriter().println("Order already exists");
                    resp.setStatus(HttpServletResponse.SC_CONFLICT);
                }
            }
        } catch (InterruptedException | SQLException | ExecutionException e) {
            throw new ServletException(e);
        }
    }
}
