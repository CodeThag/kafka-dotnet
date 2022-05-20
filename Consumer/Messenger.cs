using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MailKit.Net.Smtp;
using MailKit.Security;
using MimeKit;

namespace Consumer;

/// <summary>
/// Used to send emails
/// </summary>
public class Messenger
{
    private static readonly string Sender = "claudia24@ethereal.email";
    private static readonly string Host = "smtp.ethereal.email";
    private static readonly int Port = 587;
    private static readonly string Socket = "TLS";
    private static readonly string Password = "yc9mrJEpdyR8JksrG";

    public static async Task SendEmailAsync(string recipient, string subject, string message, CancellationToken stoppingToken)
    {
        try
        {
            var email = new MimeMessage();
            email.From.Add(MailboxAddress.Parse(Sender));

            email.To.Add(MailboxAddress.Parse(recipient));

            email.Subject = subject;
            var builder = new BodyBuilder();
            builder.HtmlBody = message;
            email.Body = builder.ToMessageBody();

            using var smtp = new SmtpClient();
            await smtp.ConnectAsync(Host, Port, Socket.Contains("SSL") ? SecureSocketOptions.SslOnConnect : SecureSocketOptions.StartTls, stoppingToken);

            await smtp.AuthenticateAsync(Sender, Password, stoppingToken);
            await smtp.SendAsync(email, stoppingToken);
            await smtp.DisconnectAsync(true, stoppingToken);

            Console.WriteLine($"Email sent: Recipient: {recipient} - Subject: {subject}");
        }
        catch (Exception ex)
        {
            Console.WriteLine(ex.Message);
        }
    }
}
